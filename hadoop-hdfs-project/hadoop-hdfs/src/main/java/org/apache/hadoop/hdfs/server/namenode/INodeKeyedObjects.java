package org.apache.hadoop.hdfs.server.namenode;

import static java.util.concurrent.TimeUnit.*;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.*;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdfs.db.DatabaseINode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class INodeKeyedObjects {
  private static IndexedCache<String, INode> cache;

  private static Set<String> concurrentUpdateSet;
  private static Set<Long> concurrentRemoveSet;
  private static long preRemoveSize = 0;
  private static long preUpdateSize = 0;

  private static ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

  static final Logger LOG = LoggerFactory.getLogger(INodeKeyedObjects.class);

  INodeKeyedObjects() {}

  public static Set<String> getBackupSet() {
    if (concurrentUpdateSet == null) {
      ConcurrentHashMap<String, Integer> map = new ConcurrentHashMap<>();
      concurrentUpdateSet = map.newKeySet();
    }
    return concurrentUpdateSet;
  }

  public static Set<Long> getRemoveSet() {
    if (concurrentRemoveSet == null) {
      ConcurrentHashMap<Long, Integer> map = new ConcurrentHashMap<>();
      concurrentRemoveSet = map.newKeySet();
    }
    return concurrentRemoveSet;
  }

  public static void asyncUpdateDB() {
    // In HDFS, the default log buffer size is 512 * 1024 bytes, or 512 KB.
    // We assume that each object size is 512 bytes, then the size of
    // concurrentUpdateSet should be 1024 which only records INode Id.
    // Note: Using INode Id, it's easy to find INode object in cache.
    int i = 0;
    final int num = 1024;
    long updateSize = concurrentUpdateSet.size();
    if (updateSize >= num) {
      Iterator<String> iterator = concurrentUpdateSet.iterator();
      if (LOG.isInfoEnabled()) {
        LOG.info("Sync files/directories from cache to database.");
      }

      List<Long> longAttr = new ArrayList<>();
      List<String> strAttr = new ArrayList<>();

      List<Long> fileIds = new ArrayList<>();
      List<String> fileAttr = new ArrayList<>();
      while (iterator.hasNext()) {
        INode inode = INodeKeyedObjects.getCache().getIfPresent(iterator.next());
        if (inode == null) continue;
        strAttr.add(inode.getLocalName());
        if (inode.getId() == 16385) {
          strAttr.add(" ");
        } else {
          strAttr.add(inode.getParentName());
        }
        longAttr.add(inode.getParentId());
        longAttr.add(inode.getId());
        longAttr.add(inode.getModificationTime());
        longAttr.add(inode.getAccessTime());
        longAttr.add(inode.getPermissionLong());
        if (inode.isDirectory()) {
          longAttr.add(0L);
        } else {
          longAttr.add(inode.asFile().getHeaderLong());
          FileUnderConstructionFeature uc = inode.asFile().getFileUnderConstructionFeature();
          if (uc != null) {
            fileIds.add(inode.getId());
            fileAttr.add(uc.getClientName(inode.getId()));
            fileAttr.add(uc.getClientMachine(inode.getId()));
          }
        }
        iterator.remove();
        if (++i >= num) break;
      }
      try {
        if (strAttr.size() > 0) {
          DatabaseINode.batchUpdateINodes(longAttr, strAttr, fileIds, fileAttr);
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
    } else {
      if (updateSize > 0 && preUpdateSize == updateSize) {
        Iterator<String> iterator = concurrentUpdateSet.iterator();
        if (LOG.isInfoEnabled()) {
          LOG.info("Propagate updated files/directories from cache to database.");
        }
        try {
          List<Long> longAttr = new ArrayList<>();
          List<String> strAttr = new ArrayList<>();
          List<Long> fileIds = new ArrayList<>();
          List<String> fileAttr = new ArrayList<>();
          while (iterator.hasNext()) {
            INode inode = INodeKeyedObjects.getCache().getIfPresent(iterator.next());
            if (inode == null) continue;
            strAttr.add(inode.getLocalName());
            if (inode.getId() == 16385) {
              strAttr.add(" ");
            } else {
              strAttr.add(inode.getParentName());
            }
            longAttr.add(inode.getParentId());
            longAttr.add(inode.getId());
            longAttr.add(inode.getModificationTime());
            longAttr.add(inode.getAccessTime());
            longAttr.add(inode.getPermissionLong());
            if (inode.isDirectory()) {
              longAttr.add(0L);
            } else {
              longAttr.add(inode.asFile().getHeaderLong());
              FileUnderConstructionFeature uc = inode.asFile().getFileUnderConstructionFeature();
              if (uc != null) {
                fileIds.add(inode.getId());
                fileAttr.add(uc.getClientName(inode.getId()));
                fileAttr.add(uc.getClientMachine(inode.getId()));
              }
            }
            iterator.remove();             
          }
          if (strAttr.size() > 0) {
            DatabaseINode.batchUpdateINodes(longAttr, strAttr, fileIds, fileAttr);
          }
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    }
    preUpdateSize = concurrentUpdateSet.size();

    List<Long> removeIds = new ArrayList<>();
    long removeSize = concurrentRemoveSet.size();
    if (removeSize >= num) {
      if (LOG.isInfoEnabled()) {
        LOG.info("Propagate removed files/directories from cache to database.");
      }
      i = 0;
      Iterator<Long> iterator = concurrentRemoveSet.iterator();
      while (iterator.hasNext()) {
        removeIds.add(iterator.next());
        iterator.remove();
        if (++i >= num) break;
      }

      try {
        if (removeIds.size() > 0) {
          DatabaseINode.batchRemoveINodes(removeIds);
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
    } else {
      if (removeSize > 0 && preRemoveSize == removeSize) {
        if (LOG.isInfoEnabled()) {
          LOG.info("Propagate removed files/directories from cache to database.");
        }
        try {
          removeIds = new ArrayList<Long>(concurrentRemoveSet);
          Iterator<Long> iterator = concurrentRemoveSet.iterator();
          while (iterator.hasNext()) {
            iterator.next();
            iterator.remove();
          }
          DatabaseINode.batchRemoveINodes(removeIds);
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    }
    preRemoveSize = concurrentRemoveSet.size();
  }

  public static void BackupSetToDB() {
    final Runnable updateToDB =
        new Runnable() {
          public void run() {
            asyncUpdateDB();
          }
        };

    // Creates and executes a periodic action that becomes enabled first after the given initial
    // delay (1s), and subsequently with the given delay (2s) between the termination of one
    // execution and the commencement of the next.
    long delay = 300L;
    String delayStr = System.getenv("UPDATE_DB_TIME_DELAY");
    if (delayStr != null) {
      delay = Long.parseLong(delayStr);
    }

    final ScheduledFuture<?> updateHandle =
        scheduler.scheduleWithFixedDelay(updateToDB, 100, delay, MICROSECONDS);

    scheduler.schedule(
        new Runnable() {
          public void run() {
            updateHandle.cancel(true);
          }
        },
        60 * 60 * 24,
        SECONDS);
  }

  // --------------------------------------------------------
  // caffeine cache

  public static IndexedCache<String, INode> getCache() {
    if (cache == null) {
      concurrentUpdateSet = ConcurrentHashMap.newKeySet();
      concurrentRemoveSet = ConcurrentHashMap.newKeySet();

      // async write updates to buffer
      BackupSetToDB();

      // Assuming each INode has 600 bytes, then
      // 10000000 * 600 / 2^30 = 5.58 GB.
      // The default object cache has 5.58 GB.
      int num = 10000000;
      String cacheNum = System.getenv("OBJECT_CACHE_SIZE");
      if (cacheNum != null) {
        num = Integer.parseInt(cacheNum);
      }

      // https://github.com/ben-manes/caffeine/wiki/Removal
      Caffeine<Object, Object> cfein =
          Caffeine.newBuilder()
              .removalListener(
                  (Object keys, Object value, RemovalCause cause) -> {
                    if (cause == RemovalCause.COLLECTED
                        || cause == RemovalCause.EXPIRED
                        || cause == RemovalCause.SIZE) {
                      if (LOG.isInfoEnabled()) {
                        LOG.info("Cache Evicted: INode = " + (String) keys);
                      }
                      // stored procedure: update inode in db
                      INode inode = (INode) value;
                      if (inode.isDirectory()) {
                        inode.asDirectory().updateINodeDirectory();
                      } else {
                        inode.asFile().updateINodeFile();
                        FileUnderConstructionFeature uc =
                            inode.asFile().getFileUnderConstructionFeature();
                        if (uc != null) {
                          uc.updateFileUnderConstruction(inode.getId());
                        }
                      }
                    }
                  })
              .maximumSize(num);
      cache =
          new IndexedCache.Builder<String, INode>()
              .buildFromCaffeine(cfein);
    }
    return cache;
  }
}
