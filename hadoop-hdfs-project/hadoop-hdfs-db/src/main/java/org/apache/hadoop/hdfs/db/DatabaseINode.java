package org.apache.hadoop.hdfs.db;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.voltdb.*;
import org.voltdb.client.*;

public class DatabaseINode {
  static final Logger LOG = LoggerFactory.getLogger(DatabaseINode.class);

  public static final long LONG_NULL = 0L;

  public DatabaseINode() {}

  public class LoadINode {
    public final long parent;
    public final String parentName;
    public final long id;
    public final String name;
    public final long permission;
    public final long modificationTime;
    public final long accessTime;
    public final long header;

    LoadINode(
        long parent,
        String parentName,
        long id,
        String name,
        long permission,
        long modificationTime,
        long accessTime,
        long header) {
      this.parent = parent;
      this.parentName = parentName;
      this.id = id;
      this.name = name;
      this.permission = permission;
      this.modificationTime = modificationTime;
      this.accessTime = accessTime;
      this.header = header;
    }

    long getParent() {
      return parent;
    }

    String getParentName() {
      return parentName;
    }

    long getId() {
      return id;
    }

    String getName() {
      return name;
    }

    long getPermission() {
      return permission;
    }

    long getModificationTime() {
      return modificationTime;
    }

    long getAccessTime() {
      return accessTime;
    }

    long getHeader() {
      return header;
    }
  }

  public LoadINode loadINode(final long id) {
    LoadINode res = null;
    try {
      DatabaseConnection obj = Database.getInstance().getConnection();
      String env = System.getenv("DATABASE");
      if (env.equals("VOLT")) {
        try {
          VoltTable[] results = obj.getVoltClient().callProcedure("LoadINode", id).getResults();
          VoltTable result = results[0];
          result.resetRowPosition();
          while (result.advanceRow()) {
            res =
                new LoadINode(
                    result.getLong(0),
                    result.getString(1),
                    result.getLong(2),
                    result.getString(3),
                    result.getLong(4),
                    result.getLong(5),
                    result.getLong(6),
                    result.getLong(7));
          }
        } catch (Exception e) {
          e.printStackTrace();
        }
      } else {
        Connection conn = obj.getConnection();
        String sql =
            "SELECT parent, parentName, id, name, permission, modificationTime, accessTime, header FROM inodes WHERE id = ?;";
        PreparedStatement pst = conn.prepareStatement(sql);
        pst.setLong(1, id);
        ResultSet rs = pst.executeQuery();
        while (rs.next()) {
          res =
              new LoadINode(
                  rs.getLong(1),
                  rs.getString(2),
                  rs.getLong(3),
                  rs.getString(4),
                  rs.getLong(5),
                  rs.getLong(6),
                  rs.getLong(7),
                  rs.getLong(8));
        }
        rs.close();
        pst.close();
      }
      Database.getInstance().retConnection(obj);
    } catch (SQLException ex) {
      System.err.println(ex.getMessage());
    }

    if (LOG.isInfoEnabled()) {
      LOG.info("Load INode [GET]: (" + id + ")");
    }
    return res;
  }

  public LoadINode loadINode(final long parentId, final String childName) {
    LoadINode res = null;
    try {
      DatabaseConnection obj = Database.getInstance().getConnection();
      String env = System.getenv("DATABASE");
      if (env.equals("VOLT")) {
        try {
          VoltTable[] results =
              obj.getVoltClient().callProcedure("LoadINodeV2", parentId, childName).getResults();
          VoltTable result = results[0];
          result.resetRowPosition();
          while (result.advanceRow()) {
            res =
                new LoadINode(
                    result.getLong(0),
                    result.getString(1),
                    result.getLong(2),
                    result.getString(3),
                    result.getLong(4),
                    result.getLong(5),
                    result.getLong(6),
                    result.getLong(7));
          }
        } catch (Exception e) {
          e.printStackTrace();
        }
      } else {
        Connection conn = obj.getConnection();
        String sql =
            "SELECT parent, parentName, id, name, permission, modificationTime, accessTime, header FROM inodes WHERE parent = ? AND name = ?;";
        PreparedStatement pst = conn.prepareStatement(sql);
        pst.setLong(1, parentId);
        pst.setString(2, childName);
        ResultSet rs = pst.executeQuery();
        while (rs.next()) {
          res =
              new LoadINode(
                  rs.getLong(1),
                  rs.getString(2),
                  rs.getLong(3),
                  rs.getString(4),
                  rs.getLong(5),
                  rs.getLong(6),
                  rs.getLong(7),
                  rs.getLong(8));
        }
        rs.close();
        pst.close();
      }
      Database.getInstance().retConnection(obj);
    } catch (SQLException ex) {
      System.err.println(ex.getMessage());
    }

    if (LOG.isInfoEnabled()) {
      LOG.info("Load INode [GET]: (" + parentId + ", " + childName + ")");
    }
    return res;
  }

  public static boolean checkInodeExistence(final long parentId, final String childName) {
    boolean exist = false;
    try {
      DatabaseConnection obj = Database.getInstance().getConnection();
      Connection conn = obj.getConnection();
      // check the existence of node in Postgres
      String sql = "SELECT COUNT(id) FROM inodes WHERE parent = ? and name = ?;";
      PreparedStatement pst = conn.prepareStatement(sql);
      pst.setLong(1, parentId);
      pst.setString(2, childName);
      ResultSet rs = pst.executeQuery();
      while (rs.next()) {
        if (rs.getInt(1) == 1) {
          exist = true;
        }
      }
      rs.close();
      pst.close();
      Database.getInstance().retConnection(obj);
    } catch (SQLException ex) {
      System.err.println(ex.getMessage());
    }
    if (LOG.isInfoEnabled()) {
      LOG.info("checkInodeExistence [GET]: (" + parentId + "," + childName + "," + exist + ")");
    }
    return exist;
  }

  public static boolean checkInodeExistence(final long childId) {
    boolean exist = false;
    try {
      DatabaseConnection obj = Database.getInstance().getConnection();
      Connection conn = obj.getConnection();
      // check the existence of node in Postgres
      String sql = "SELECT COUNT(id) FROM inodes WHERE id = ?;";
      PreparedStatement pst = conn.prepareStatement(sql);
      pst.setLong(1, childId);
      ResultSet rs = pst.executeQuery();
      while (rs.next()) {
        if (rs.getInt(1) == 1) {
          exist = true;
        }
      }
      rs.close();
      pst.close();
      Database.getInstance().retConnection(obj);
    } catch (SQLException ex) {
      System.err.println(ex.getMessage());
    }
    if (LOG.isInfoEnabled()) {
      LOG.info("checkInodeExistence [GET]: (" + childId + "," + exist + ")");
    }
    return exist;
  }

  private static <T> void setAttribute(final long id, final String attrName, final T attrValue) {
    try {
      DatabaseConnection obj = Database.getInstance().getConnection();
      Connection conn = obj.getConnection();

      String sql = "UPDATE inodes SET " + attrName + " = ? WHERE id = ?;";
      PreparedStatement pst = conn.prepareStatement(sql);

      if (attrValue instanceof String) {
        if (attrValue.toString() == null) {
          pst.setNull(1, java.sql.Types.VARCHAR);
        } else {
          pst.setString(1, attrValue.toString());
        }
      } else if (attrValue instanceof Integer || attrValue instanceof Long) {
        pst.setLong(1, ((Long) attrValue).longValue());
      } else {
        System.err.println("Only support string and long types for now.");
        System.exit(-1);
      }
      pst.setLong(2, id);

      pst.executeUpdate();
      pst.close();
      Database.getInstance().retConnection(obj);
    } catch (SQLException ex) {
      System.err.println(ex.getMessage());
    }
    if (LOG.isInfoEnabled()) {
      LOG.info(attrName + " [UPDATE]: (" + id + "," + attrValue + ")");
    }
  }

  private static <T> T getAttribute(final long id, final String attrName) {
    T result = null;
    try {
      DatabaseConnection obj = Database.getInstance().getConnection();
      Connection conn = obj.getConnection();
      String sql = "SELECT " + attrName + " FROM inodes WHERE id = ?;";
      PreparedStatement pst = conn.prepareStatement(sql);
      pst.setLong(1, id);
      ResultSet rs = pst.executeQuery();
      while (rs.next()) {
        ResultSetMetaData rsmd = rs.getMetaData();
        if (rsmd.getColumnType(1) == Types.BIGINT || rsmd.getColumnType(1) == Types.INTEGER) {
          result = (T) Long.valueOf(rs.getLong(1));
        } else if (rsmd.getColumnType(1) == Types.VARCHAR) {
          result = (T) rs.getString(1);
        }
      }
      rs.close();
      pst.close();
      Database.getInstance().retConnection(obj);
    } catch (SQLException ex) {
      System.err.println(ex.getMessage());
    }
    if (LOG.isInfoEnabled()) {
      LOG.info(attrName + " [GET]: (" + id + "," + result + ")");
    }

    return result;
  }

  public static void insertInode(
      final long id,
      final long pid,
      final String name,
      final long accessTime,
      final long modificationTime,
      final long permission,
      final long header,
      final String parentName) {
    try {
      DatabaseConnection obj = Database.getInstance().getConnection();
      String env = System.getenv("DATABASE");
      if (env.equals("VOLT")) {
        try {
          obj.getVoltClient()
              .callProcedure(
                  new NullCallback(),
                  "InsertINode",
                  id,
                  pid,
                  name,
                  accessTime,
                  modificationTime,
                  permission,
                  header,
                  parentName);
        } catch (Exception e) {
          e.printStackTrace();
        }
      } else {
        String sql =
            "INSERT INTO inodes("
                + " id, name, accessTime, modificationTime, permission, header, parent"
                + ") VALUES (?, ?, ?, ?, ?, ?, ?) ON CONFLICT(id) DO UPDATE"
                + "SET name = ?, accessTime = ?, modificationTime = ?, permission = ?, header = ?, parent = ?;";
        Connection conn = obj.getConnection();
        PreparedStatement pst = conn.prepareStatement(sql);

        pst.setLong(1, id);
        if (name == null) {
          pst.setNull(2, java.sql.Types.VARCHAR);
        } else {
          pst.setString(2, name);
        }
        pst.setLong(3, accessTime);
        pst.setLong(4, modificationTime);
        pst.setLong(5, permission);
        pst.setLong(6, header);
        pst.setLong(7, pid);

        if (name == null) {
          pst.setNull(8, java.sql.Types.VARCHAR);
        } else {
          pst.setString(8, name);
        }
        pst.setLong(9, accessTime);
        pst.setLong(10, modificationTime);
        pst.setLong(11, permission);
        pst.setLong(12, header);
        pst.setLong(13, pid);

        pst.executeUpdate();
        pst.close();
      }

      Database.getInstance().retConnection(obj);
    } catch (SQLException ex) {
      System.err.println(ex.getMessage());
    }
    if (LOG.isInfoEnabled()) {
      LOG.info("insertInode: (" + id + ")");
    }
  }

  public static void renameInode(
      final long id,
      final long pid,
      final String name,
      final long accessTime,
      final long modificationTime,
      final long permission,
      final long header,
      final String parentName) {
    try {
      DatabaseConnection obj = Database.getInstance().getConnection();
      String env = System.getenv("DATABASE");
      if (env.equals("VOLT")) {
        try {
          obj.getVoltClient()
              .callProcedure(
                  new NullCallback(),
                  "RenameINode",
                  id,
                  pid,
                  name,
                  accessTime,
                  modificationTime,
                  permission,
                  header,
                  parentName);
        } catch (Exception e) {
          e.printStackTrace();
        }
      } else {
        throw new SQLException("[UNSUPPORT] Invalid operation ...");
      }

      Database.getInstance().retConnection(obj);
    } catch (SQLException ex) {
      System.err.println(ex.getMessage());
    }
    if (LOG.isInfoEnabled()) {
      LOG.info("renameInode: (" + id + ")");
    }
  }

  public static void setAccessTime(final long id, final long accessTime) {
    try {
      DatabaseConnection obj = Database.getInstance().getConnection();
      String env = System.getenv("DATABASE");
      if (env.equals("VOLT")) {
        try {
          obj.getVoltClient().callProcedure(new NullCallback(), "SetAccessTime", id, accessTime);
        } catch (Exception e) {
          e.printStackTrace();
        }
      } else {
        Connection conn = obj.getConnection();
        String sql = "UPDATE inodes SET accessTime = ? WHERE id = ?;";
        PreparedStatement pst = conn.prepareStatement(sql);
        pst.setLong(1, accessTime);
        pst.setLong(2, id);
        pst.executeUpdate();
        pst.close();
      }
      Database.getInstance().retConnection(obj);
    } catch (SQLException ex) {
      System.err.println(ex.getMessage());
    }
    if (LOG.isInfoEnabled()) {
      LOG.info("accessTime [UPDATE]: (" + id + "," + accessTime + ")");
    }
  }

  public static void setModificationTime(final long id, final long modificationTime) {
    try {
      DatabaseConnection obj = Database.getInstance().getConnection();
      String env = System.getenv("DATABASE");
      if (env.equals("VOLT")) {
        try {
          obj.getVoltClient()
              .callProcedure(new NullCallback(), "SetModificationTime", id, modificationTime);
        } catch (Exception e) {
          e.printStackTrace();
        }
      } else {
        Connection conn = obj.getConnection();
        String sql = "UPDATE inodes SET modificationTime = ? WHERE id = ?;";
        PreparedStatement pst = conn.prepareStatement(sql);
        pst.setLong(1, modificationTime);
        pst.setLong(2, id);
        pst.executeUpdate();
        pst.close();
      }
      Database.getInstance().retConnection(obj);
    } catch (SQLException ex) {
      System.err.println(ex.getMessage());
    }
    if (LOG.isInfoEnabled()) {
      LOG.info("modificationTime [UPDATE]: (" + id + "," + modificationTime + ")");
    }
  }

  public static void updateModificationTime(final long id, final long childId) {
    try {
      DatabaseConnection obj = Database.getInstance().getConnection();
      String env = System.getenv("DATABASE");
      if (env.equals("VOLT")) {
        try {
          obj.getVoltClient()
              .callProcedure(new NullCallback(), "UpdateModificationTime", id, childId);
        } catch (Exception e) {
          e.printStackTrace();
        }
      } else {
        Connection conn = obj.getConnection();
        String sql =
            "UPDATE inodes SET modificationTime = ("
                + "SELECT modificationTime FROM inodes WHERE id = ?) WHERE id = ?;";
        PreparedStatement pst = conn.prepareStatement(sql);
        pst.setLong(1, childId);
        pst.setLong(2, id);
        pst.executeUpdate();
        pst.close();
      }
      Database.getInstance().retConnection(obj);
    } catch (SQLException ex) {
      System.err.println(ex.getMessage());
    }
    if (LOG.isInfoEnabled()) {
      LOG.info("updateModificationTime [UPDATE]: (" + id + ")");
    }
  }

  public static void setPermission(final long id, final long permission) {
    try {
      DatabaseConnection obj = Database.getInstance().getConnection();
      String env = System.getenv("DATABASE");
      if (env.equals("VOLT")) {
        try {
          obj.getVoltClient().callProcedure(new NullCallback(), "SetPermission", id, permission);
        } catch (Exception e) {
          e.printStackTrace();
        }
      } else {
        Connection conn = obj.getConnection();
        String sql = "UPDATE inodes SET permission = ? WHERE id = ?;";
        PreparedStatement pst = conn.prepareStatement(sql);
        pst.setLong(1, permission);
        pst.setLong(2, id);
        pst.executeUpdate();
        pst.close();
      }
      Database.getInstance().retConnection(obj);
    } catch (SQLException ex) {
      System.err.println(ex.getMessage());
    }
    if (LOG.isInfoEnabled()) {
      LOG.info("permission [UPDATE]: (" + id + "," + permission + ")");
    }
  }

  public static void setHeader(final long id, final long header) {
    try {
      DatabaseConnection obj = Database.getInstance().getConnection();
      String env = System.getenv("DATABASE");
      if (env.equals("VOLT")) {
        try {
          obj.getVoltClient().callProcedure(new NullCallback(), "SetHeader", id, header);
        } catch (Exception e) {
          e.printStackTrace();
        }
      } else {
        Connection conn = obj.getConnection();
        String sql = "UPDATE inodes SET header = ? WHERE id = ?;";
        PreparedStatement pst = conn.prepareStatement(sql);
        pst.setLong(1, header);
        pst.setLong(2, id);
        pst.executeUpdate();
        pst.close();
      }
      Database.getInstance().retConnection(obj);
    } catch (SQLException ex) {
      System.err.println(ex.getMessage());
    }
    if (LOG.isInfoEnabled()) {
      LOG.info("header [UPDATE]: (" + id + "," + header + ")");
    }
  }

  public static void setParent(final long id, final long parent) {
    try {
      DatabaseConnection obj = Database.getInstance().getConnection();
      String env = System.getenv("DATABASE");
      if (env.equals("VOLT")) {
        try {
          obj.getVoltClient().callProcedure(new NullCallback(), "SetParent", id, parent);
        } catch (Exception e) {
          e.printStackTrace();
        }
      } else {
        Connection conn = obj.getConnection();
        String sql = "UPDATE inodes SET parent = ? WHERE id = ?;";
        PreparedStatement pst = conn.prepareStatement(sql);
        pst.setLong(1, parent);
        pst.setLong(2, id);
        pst.executeUpdate();
        pst.close();
      }
      Database.getInstance().retConnection(obj);
    } catch (SQLException ex) {
      System.err.println(ex.getMessage());
    }
    if (LOG.isInfoEnabled()) {
      LOG.info("parent [UPDATE]: (" + id + "," + parent + ")");
    }
  }

  public static void setParents(final long oldparent, final long newparent) {
    try {
      DatabaseConnection obj = Database.getInstance().getConnection();
      String env = System.getenv("DATABASE");
      if (env.equals("VOLT")) {
        try {
          obj.getVoltClient().callProcedure(new NullCallback(), "SetParents", oldparent, newparent);
        } catch (Exception e) {
          e.printStackTrace();
        }
      } else {
        // Connection conn = obj.getConnection();
        // String sql = "UPDATE inodes SET parent = ? WHERE id = ?;";
        // PreparedStatement pst = conn.prepareStatement(sql);
        // if (int i = 0; i < ids.length; i++) {
        //   pst.setLong(1, parent);
        //   pst.setLong(2, ids[i]);
        //   pst.addBatch();
        // }
        // pst.executeBatch();
        // pst.close();
        throw new SQLException("[UNSUPPORT] Invalid operation ...");
      }
      Database.getInstance().retConnection(obj);
    } catch (SQLException ex) {
      System.err.println(ex.getMessage());
    }
    if (LOG.isInfoEnabled()) {
      LOG.info("parent [UPDATE]: (childs," + oldparent + ") to " + "(childs," + newparent + ")");
    }
  }

  public static void setName(final long id, final String name) {
    try {
      DatabaseConnection obj = Database.getInstance().getConnection();
      String env = System.getenv("DATABASE");
      if (env.equals("VOLT")) {
        try {
          obj.getVoltClient().callProcedure(new NullCallback(), "SetName", id, name);
        } catch (Exception e) {
          e.printStackTrace();
        }
      } else {
        Connection conn = obj.getConnection();
        String sql = "UPDATE inodes SET name = ? WHERE id = ?;";
        PreparedStatement pst = conn.prepareStatement(sql);
        pst.setString(1, name);
        pst.setLong(2, id);
        pst.executeUpdate();
        pst.close();
      }
      Database.getInstance().retConnection(obj);
    } catch (SQLException ex) {
      System.err.println(ex.getMessage());
    }
    if (LOG.isInfoEnabled()) {
      LOG.info("name [UPDATE]: (" + id + "," + name + ")");
    }
  }

  public static long getAccessTime(final long id) {
    long res = 0;
    try {
      DatabaseConnection obj = Database.getInstance().getConnection();
      String env = System.getenv("DATABASE");
      if (env.equals("VOLT")) {
        try {
          VoltTable[] results = obj.getVoltClient().callProcedure("GetAccessTime", id).getResults();
          VoltTable result = results[0];
          result.resetRowPosition();
          while (result.advanceRow()) {
            res = result.getLong(0);
          }
        } catch (Exception e) {
          e.printStackTrace();
        }
      } else {
        Connection conn = obj.getConnection();
        String sql = "SELECT accessTime FROM inodes WHERE id = ?;";
        PreparedStatement pst = conn.prepareStatement(sql);
        pst.setLong(1, id);
        ResultSet rs = pst.executeQuery();
        while (rs.next()) {
          res = rs.getLong(1);
        }
        rs.close();
        pst.close();
      }
      Database.getInstance().retConnection(obj);
    } catch (SQLException ex) {
      System.err.println(ex.getMessage());
    }

    if (LOG.isInfoEnabled()) {
      LOG.info("accessTime [GET]: (" + id + "," + res + ")");
    }
    return res;
  }

  public static long getModificationTime(final long id) {
    long res = 0;
    try {
      DatabaseConnection obj = Database.getInstance().getConnection();
      String env = System.getenv("DATABASE");
      if (env.equals("VOLT")) {
        try {
          VoltTable[] results =
              obj.getVoltClient().callProcedure("GetModificationTime", id).getResults();
          VoltTable result = results[0];
          result.resetRowPosition();
          while (result.advanceRow()) {
            res = result.getLong(0);
          }
        } catch (Exception e) {
          e.printStackTrace();
        }
      } else {
        Connection conn = obj.getConnection();
        String sql = "SELECT modificationTime FROM inodes WHERE id = ?;";
        PreparedStatement pst = conn.prepareStatement(sql);
        pst.setLong(1, id);
        ResultSet rs = pst.executeQuery();
        while (rs.next()) {
          res = rs.getLong(1);
        }
        rs.close();
        pst.close();
      }
      Database.getInstance().retConnection(obj);
    } catch (SQLException ex) {
      System.err.println(ex.getMessage());
    }

    if (LOG.isInfoEnabled()) {
      LOG.info("modificationTime [GET]: (" + id + "," + res + ")");
    }
    return res;
  }

  public static long getHeader(final long id) {
    long res = 0;
    try {
      DatabaseConnection obj = Database.getInstance().getConnection();
      String env = System.getenv("DATABASE");
      if (env.equals("VOLT")) {
        try {
          VoltTable[] results = obj.getVoltClient().callProcedure("GetHeader", id).getResults();
          VoltTable result = results[0];
          result.resetRowPosition();
          while (result.advanceRow()) {
            res = result.getLong(0);
          }
        } catch (Exception e) {
          e.printStackTrace();
        }
      } else {
        Connection conn = obj.getConnection();
        String sql = "SELECT header FROM inodes WHERE id = ?;";
        PreparedStatement pst = conn.prepareStatement(sql);
        pst.setLong(1, id);
        ResultSet rs = pst.executeQuery();
        while (rs.next()) {
          res = rs.getLong(1);
        }
        rs.close();
        pst.close();
      }
      Database.getInstance().retConnection(obj);
    } catch (SQLException ex) {
      System.err.println(ex.getMessage());
    }

    if (LOG.isInfoEnabled()) {
      LOG.info("header [GET]: (" + id + "," + res + ")");
    }
    return res;
  }

  public static long getPermission(final long id) {
    long res = 0;
    try {
      DatabaseConnection obj = Database.getInstance().getConnection();
      String env = System.getenv("DATABASE");
      if (env.equals("VOLT")) {
        try {
          VoltTable[] results = obj.getVoltClient().callProcedure("GetPermission", id).getResults();
          VoltTable result = results[0];
          result.resetRowPosition();
          while (result.advanceRow()) {
            res = result.getLong(0);
          }
        } catch (Exception e) {
          e.printStackTrace();
        }
      } else {
        Connection conn = obj.getConnection();
        String sql = "SELECT permission FROM inodes WHERE id = ?;";
        PreparedStatement pst = conn.prepareStatement(sql);
        pst.setLong(1, id);
        ResultSet rs = pst.executeQuery();
        while (rs.next()) {
          res = rs.getLong(1);
        }
        rs.close();
        pst.close();
      }
      Database.getInstance().retConnection(obj);
    } catch (SQLException ex) {
      System.err.println(ex.getMessage());
    }

    if (LOG.isInfoEnabled()) {
      LOG.info("permission [GET]: (" + id + "," + res + ")");
    }
    return res;
  }

  public static long getParent(final long id) {
    long res = 0;
    try {
      DatabaseConnection obj = Database.getInstance().getConnection();
      String env = System.getenv("DATABASE");
      if (env.equals("VOLT")) {
        try {
          VoltTable[] results = obj.getVoltClient().callProcedure("GetParent", id).getResults();
          VoltTable result = results[0];
          result.resetRowPosition();
          while (result.advanceRow()) {
            res = result.getLong(0);
          }
        } catch (Exception e) {
          e.printStackTrace();
        }
      } else {
        Connection conn = obj.getConnection();
        String sql = "SELECT parent FROM inodes WHERE id = ?;";
        PreparedStatement pst = conn.prepareStatement(sql);
        pst.setLong(1, id);
        ResultSet rs = pst.executeQuery();
        while (rs.next()) {
          res = rs.getLong(1);
        }
        rs.close();
        pst.close();
      }
      Database.getInstance().retConnection(obj);
    } catch (SQLException ex) {
      System.err.println(ex.getMessage());
    }

    if (LOG.isInfoEnabled()) {
      LOG.info("parent [GET]: (" + id + "," + res + ")");
    }
    return res;
  }

  public static String getName(final long id) {
    String res = null;
    try {
      DatabaseConnection obj = Database.getInstance().getConnection();
      String env = System.getenv("DATABASE");
      if (env.equals("VOLT")) {
        try {
          VoltTable[] results = obj.getVoltClient().callProcedure("GetName", id).getResults();
          VoltTable result = results[0];
          result.resetRowPosition();
          while (result.advanceRow()) {
            res = result.getString(0);
          }
        } catch (Exception e) {
          e.printStackTrace();
        }
      } else {
        Connection conn = obj.getConnection();
        String sql = "SELECT name FROM inodes WHERE id = ?;";
        PreparedStatement pst = conn.prepareStatement(sql);
        pst.setLong(1, id);
        ResultSet rs = pst.executeQuery();
        while (rs.next()) {
          res = rs.getString(1);
        }
        rs.close();
        pst.close();
      }
      Database.getInstance().retConnection(obj);
    } catch (SQLException ex) {
      System.err.println(ex.getMessage());
    }

    if (LOG.isInfoEnabled()) {
      LOG.info("name [GET]: (" + id + "," + res + ")");
    }
    return res;
  }

  public static String getParentName(final long id) {
    String res = null;
    try {
      DatabaseConnection obj = Database.getInstance().getConnection();
      String env = System.getenv("DATABASE");
      if (env.equals("VOLT")) {
        try {
          VoltTable[] results = obj.getVoltClient().callProcedure("GetParentName", id).getResults();
          VoltTable result = results[0];
          result.resetRowPosition();
          while (result.advanceRow()) {
            res = result.getString(0);
          }
        } catch (Exception e) {
          e.printStackTrace();
        }
      } else {
        Connection conn = obj.getConnection();
        String sql = "SELECT parentName FROM inodes WHERE id = ?;";
        PreparedStatement pst = conn.prepareStatement(sql);
        pst.setLong(1, id);
        ResultSet rs = pst.executeQuery();
        while (rs.next()) {
          res = rs.getString(1);
        }
        rs.close();
        pst.close();
      }
      Database.getInstance().retConnection(obj);
    } catch (SQLException ex) {
      System.err.println(ex.getMessage());
    }

    if (LOG.isInfoEnabled()) {
      LOG.info("parent name [GET]: (" + id + "," + res + ")");
    }
    return res;
  }

  public static long getChild(final long parentId, final String childName) {
    long childId = -1;
    try {
      DatabaseConnection obj = Database.getInstance().getConnection();
      String env = System.getenv("DATABASE");
      if (env.equals("VOLT")) {
        try {
          VoltTable[] results =
              obj.getVoltClient().callProcedure("GetChild", parentId, childName).getResults();
          VoltTable result = results[0];
          result.resetRowPosition();
          while (result.advanceRow()) {
            childId = result.getLong(0);
          }
        } catch (Exception e) {
          e.printStackTrace();
        }
      } else {
        Connection conn = obj.getConnection();
        // check the existence of node in Postgres
        String sql = "SELECT id FROM inodes WHERE parent = ? AND name = ?;";
        PreparedStatement pst = conn.prepareStatement(sql);
        pst.setLong(1, parentId);
        pst.setString(2, childName);
        ResultSet rs = pst.executeQuery();
        while (rs.next()) {
          childId = rs.getLong(1);
        }
        rs.close();
        pst.close();
      }
      Database.getInstance().retConnection(obj);
    } catch (SQLException ex) {
      System.out.println(ex.getMessage());
    }

    if (LOG.isInfoEnabled()) {
      LOG.info("getChild: (" + childId + "," + parentId + "," + childName + ")");
    }

    return childId;
  }

  public static List<Long> getChildIdsByPath(final long id, final String[] components) {
    List<Long> res = new ArrayList();
    try {
      // call a stored procedure
      DatabaseConnection obj = Database.getInstance().getConnection();
      try {
        VoltTable[] results =
            obj.getVoltClient().callProcedure("GetChildIdsByPath", id, components).getResults();
        VoltTable result = results[0];
        result.resetRowPosition();
        while (result.advanceRow()) {
          res.add(result.getLong(0));
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
      Database.getInstance().retConnection(obj);
    } catch (Exception e) {
      e.printStackTrace();
    }
    if (LOG.isInfoEnabled()) {
      LOG.info("getChildIdsByPath: " + id);
    }

    return res;
  }

  public static void removeChild(final long id) {
    try {
      DatabaseConnection obj = Database.getInstance().getConnection();
      String env = System.getenv("DATABASE");
      if (env.equals("VOLT")) {
        // call a stored procedure
        try {
          obj.getVoltClient().callProcedure(new NullCallback(), "RemoveChild", id);
        } catch (Exception e) {
          e.printStackTrace();
        }
      } else {
        Connection conn = obj.getConnection();
        // delete file/directory recusively
        String sql =
            "DELETE FROM inodes WHERE id IN ("
                + "   WITH RECURSIVE cte AS ("
                + "       SELECT id, parent FROM inodes d WHERE id = ?"
                + "   UNION ALL"
                + "       SELECT d.id, d.parent FROM cte"
                + "       JOIN inodes d ON cte.id = d.parent"
                + "   )"
                + "   SELECT id FROM cte"
                + ");";
        PreparedStatement pst = conn.prepareStatement(sql);
        pst.setLong(1, id);
        pst.executeUpdate();
        pst.close();
      }
      Database.getInstance().retConnection(obj);
    } catch (SQLException ex) {
      System.err.println(ex.getMessage());
    }
    if (LOG.isInfoEnabled()) {
      LOG.info("removeChild: " + id);
    }
  }

  public static List<String> getPathComponents(final long childId) {
    List<String> names = new ArrayList();
    try {
      DatabaseConnection obj = Database.getInstance().getConnection();
      Connection conn = obj.getConnection();
      String sql =
          "WITH RECURSIVE cte AS ("
              + "       SELECT id, parent, name FROM inodes d WHERE id = ?"
              + "   UNION ALL"
              + "       SELECT d.id, d.parent, d.name FROM cte"
              + "   JOIN inodes d ON cte.parent = d.id"
              + ") SELECT name FROM cte;";
      PreparedStatement pst = conn.prepareStatement(sql);
      pst.setLong(1, childId);
      ResultSet rs = pst.executeQuery();
      while (rs.next()) {
        names.add(0, rs.getString(1));
      }
      rs.close();
      pst.close();
      Database.getInstance().retConnection(obj);
    } catch (SQLException ex) {
      System.err.println(ex.getMessage());
    }
    if (LOG.isInfoEnabled()) {
      LOG.info("getPathComponents: " + childId);
    }
    return names;
  }

  // Inclusive: childId
  public static Pair<List<Long>, List<String>> getParentIdsAndPaths(final long childId) {
    List<Long> ids = new ArrayList();
    List<String> names = new ArrayList();
    ImmutablePair result = null;
    try {
      DatabaseConnection obj = Database.getInstance().getConnection();
      Connection conn = obj.getConnection();
      String sql =
          "WITH RECURSIVE cte AS ("
              + "       SELECT id, parent, name FROM inodes d WHERE id = ?"
              + "   UNION ALL"
              + "       SELECT d.id, d.parent, d.name FROM cte"
              + "   JOIN inodes d ON cte.parent = d.id"
              + ") SELECT parent, name FROM cte;";
      PreparedStatement pst = conn.prepareStatement(sql);
      pst.setLong(1, childId);
      ResultSet rs = pst.executeQuery();
      while (rs.next()) {
        ids.add(0, rs.getLong(1));
        names.add(0, rs.getString(2));
      }
      rs.close();
      pst.close();
      Database.getInstance().retConnection(obj);
    } catch (SQLException ex) {
      System.err.println(ex.getMessage());
    }

    if (LOG.isInfoEnabled()) {
      LOG.info("getParentIdsAndPaths: " + childId);
    }

    if (ids.size() != 0 || names.size() != 0) {
      result = new ImmutablePair<>(ids, names);
    }
    return result;
  }

  // Exclusive: childId
  public static List<Long> getParentIds(final long childId) {
    List<Long> parents = new ArrayList();
    try {
      DatabaseConnection obj = Database.getInstance().getConnection();
      Connection conn = obj.getConnection();
      String sql =
          "WITH RECURSIVE cte AS ("
              + "       SELECT id, parent FROM inodes d WHERE id = ?"
              + "   UNION ALL"
              + "       SELECT d.id, d.parent FROM cte"
              + "   JOIN inodes d ON cte.parent = d.id"
              + ") SELECT id FROM cte WHERE id != ?;";
      PreparedStatement pst = conn.prepareStatement(sql);
      pst.setLong(1, childId);
      pst.setLong(2, childId);
      ResultSet rs = pst.executeQuery();
      while (rs.next()) {
        parents.add(0, rs.getLong(1));
      }
      rs.close();
      pst.close();
      Database.getInstance().retConnection(obj);
    } catch (SQLException ex) {
      System.err.println(ex.getMessage());
    }
    if (LOG.isInfoEnabled()) {
      LOG.info("getParentIds: " + childId);
    }
    return parents;
  }

  public static List<Long> getChildIds(final long childId) {
    List<Long> childIds = new ArrayList();
    try {
      DatabaseConnection obj = Database.getInstance().getConnection();
      Connection conn = obj.getConnection();
      String sql =
          "WITH RECURSIVE cte AS ("
              + "       SELECT id, parent FROM inodes d WHERE id = ?"
              + "   UNION ALL"
              + "       SELECT d.id, d.parent FROM cte"
              + "   JOIN inodes d ON cte.id = d.parent"
              + ") SELECT id FROM cte;";
      PreparedStatement pst = conn.prepareStatement(sql);
      pst.setLong(1, childId);
      ResultSet rs = pst.executeQuery();
      while (rs.next()) {
        childIds.add(0, rs.getLong(1));
      }
      rs.close();
      pst.close();
      Database.getInstance().retConnection(obj);
    } catch (SQLException ex) {
      System.err.println(ex.getMessage());
    }
    if (LOG.isInfoEnabled()) {
      LOG.info("getChildIds: " + childId);
    }
    return childIds;
  }

  public static List<Long> getChildrenIds(final long parentId) {
    List<Long> childIds = new ArrayList<>();
    try {
      DatabaseConnection obj = Database.getInstance().getConnection();
      String env = System.getenv("DATABASE");

      if (env.equals("VOLT")) {
        try {
          VoltTable[] results =
              obj.getVoltClient().callProcedure("GetChildrenIds", parentId).getResults();
          VoltTable result = results[0];
          result.resetRowPosition();
          while (result.advanceRow()) {
            childIds.add(result.getLong(0));
          }
        } catch (Exception e) {
          e.printStackTrace();
        }
      } else {
        Connection conn = obj.getConnection();
        // check the existence of node in Postgres
        String sql = "SELECT id FROM inodes WHERE parent = ?;";
        PreparedStatement pst = conn.prepareStatement(sql);
        pst.setLong(1, parentId);
        ResultSet rs = pst.executeQuery();
        while (rs.next()) {
          long id = rs.getLong(1);
          childIds.add(id);
        }
        rs.close();
        pst.close();
      }
      Database.getInstance().retConnection(obj);
    } catch (SQLException ex) {
      System.out.println(ex.getMessage());
    }

    if (LOG.isInfoEnabled()) {
      LOG.info("getChildrenIds: (" + childIds + "," + parentId + ")");
    }

    return childIds;
  }

  public static List<String> getChildrenNames(final long parentId) {
    List<String> childNames = new ArrayList<>();
    try {
      DatabaseConnection obj = Database.getInstance().getConnection();
      String env = System.getenv("DATABASE");

      if (env.equals("VOLT")) {
        try {
          VoltTable[] results =
              obj.getVoltClient().callProcedure("GetChildrenNames", parentId).getResults();
          VoltTable result = results[0];
          result.resetRowPosition();
          while (result.advanceRow()) {
            childNames.add(result.getString(0));
          }
        } catch (Exception e) {
          e.printStackTrace();
        }
      } else {
        Connection conn = obj.getConnection();
        // check the existence of node in Postgres
        String sql = "SELECT name FROM inodes WHERE parent = ?;";
        PreparedStatement pst = conn.prepareStatement(sql);
        pst.setLong(1, parentId);
        ResultSet rs = pst.executeQuery();
        while (rs.next()) {
          childNames.add(rs.getString(1));
        }
        rs.close();
        pst.close();
      }
      Database.getInstance().retConnection(obj);
    } catch (SQLException ex) {
      System.out.println(ex.getMessage());
    }

    if (LOG.isInfoEnabled()) {
      LOG.info("getChildrenNames: (" + parentId + ")");
    }

    return childNames;
  }

  public static boolean addChild(final long childId, final String childName, final long parentId) {
    try {
      DatabaseConnection obj = Database.getInstance().getConnection();
      String env = System.getenv("DATABASE");
      if (env.equals("VOLT")) {
        try {
          obj.getVoltClient()
              .callProcedure(new NullCallback(), "AddChild", childId, childName, parentId);
        } catch (Exception e) {
          e.printStackTrace();
        }
      } else {
        Connection conn = obj.getConnection();
        String sql =
            "INSERT INTO inodes(parent, name, id) VALUES (?, ?, ?) ON CONFLICT(id) DO UPDATE SET parent = ?, name = ?;";
        PreparedStatement pst = conn.prepareStatement(sql);
        pst.setLong(1, parentId);
        pst.setString(2, childName);
        pst.setLong(3, childId);
        pst.setLong(4, parentId);
        pst.setString(5, childName);
        pst.executeUpdate();
        pst.close();
      }
      Database.getInstance().retConnection(obj);
    } catch (SQLException ex) {
      System.out.println(ex.getMessage());
    }
    if (LOG.isInfoEnabled()) {
      LOG.info("addChild: [OK] UPSERT (" + childId + "," + parentId + "," + childName + ")");
    }
    return true;
  }

  public static long getINodesNum() {
    long num = 0;
    try {
      DatabaseConnection obj = Database.getInstance().getConnection();
      Connection conn = obj.getConnection();
      String sql = "SELECT COUNT(id) FROM inodes;";
      Statement st = conn.createStatement();
      ResultSet rs = st.executeQuery(sql);
      while (rs.next()) {
        num = rs.getLong(1);
      }
      rs.close();
      st.close();
      Database.getInstance().retConnection(obj);
    } catch (SQLException ex) {
      System.err.println(ex.getMessage());
    }
    if (LOG.isInfoEnabled()) {
      LOG.info("getINodesNum [GET]: (" + num + ")");
    }

    return num;
  }

  public static long getLastInodeId() {
    long num = 0;
    try {
      DatabaseConnection obj = Database.getInstance().getConnection();
      Connection conn = obj.getConnection();
      String sql = "SELECT MAX(id) FROM inodes;";
      Statement st = conn.createStatement();
      ResultSet rs = st.executeQuery(sql);
      while (rs.next()) {
        num = rs.getLong(1);
      }
      rs.close();
      st.close();
      Database.getInstance().retConnection(obj);
    } catch (SQLException ex) {
      System.err.println(ex.getMessage());
    }
    if (LOG.isInfoEnabled()) {
      LOG.info("getLastInodeId [GET]: (" + num + ")");
    }
    return num;
  }

  public static void insertUc(final long id, final String clientName, final String clientMachine) {
    try {
      DatabaseConnection obj = Database.getInstance().getConnection();
      String env = System.getenv("DATABASE");
      if (env.equals("VOLT")) {
        try {
          obj.getVoltClient()
              .callProcedure(new NullCallback(), "InsertUc", id, clientName, clientMachine);
        } catch (Exception e) {
          e.printStackTrace();
        }
      } else {
        Connection conn = obj.getConnection();
        String sql = "INSERT INTO inodeuc(id, clientName, clientMachine) VALUES (?, ?, ?);";
        PreparedStatement pst = conn.prepareStatement(sql);
        pst.setLong(1, id);
        pst.setString(2, clientName);
        pst.setString(3, clientMachine);
        pst.executeUpdate();
        pst.close();
      }
      Database.getInstance().retConnection(obj);
    } catch (SQLException ex) {
      System.err.println(ex.getMessage());
    }
    if (LOG.isInfoEnabled()) {
      LOG.info("insertUc [UPDATE]: (" + id + ", " + clientName + ", " + clientMachine + ")");
    }
  }

  public static Boolean checkUCExistence(final long id) {
    boolean exist = false;
    try {
      DatabaseConnection obj = Database.getInstance().getConnection();
      String env = System.getenv("DATABASE");
      if (env.equals("VOLT")) {
        try {
          VoltTable[] results =
              obj.getVoltClient().callProcedure("CheckUCExistence", id).getResults();
          VoltTable result = results[0];
          result.resetRowPosition();
          while (result.advanceRow()) {
            if (result.getLong(0) >= 1) {
              exist = true;
            }
          }
        } catch (Exception e) {
          e.printStackTrace();
        }
      } else {
        Connection conn = obj.getConnection();
        String sql = "SELECT COUNT(id) FROM inodeuc WHERE id = ?";
        PreparedStatement pst = conn.prepareStatement(sql);
        pst.setLong(1, id);
        ResultSet rs = pst.executeQuery();
        while (rs.next()) {
          if (rs.getInt(1) == 1) {
            exist = true;
          }
        }
        rs.close();
        pst.close();
      }
      Database.getInstance().retConnection(obj);
    } catch (SQLException ex) {
      System.err.println(ex.getMessage());
    }
    if (LOG.isInfoEnabled()) {
      LOG.info("checkUCExistence [GET]: (" + id + ", " + exist + ")");
    }
    return exist;
  }

  public static String getUcClientName(final long id) {
    String name = null;
    try {
      DatabaseConnection obj = Database.getInstance().getConnection();
      String env = System.getenv("DATABASE");

      if (env.equals("VOLT")) {
        try {
          VoltTable[] results =
              obj.getVoltClient().callProcedure("GetUcClientName", id).getResults();
          VoltTable result = results[0];
          result.resetRowPosition();
          while (result.advanceRow()) {
            name = result.getString(0);
          }
        } catch (Exception e) {
          e.printStackTrace();
        }
      } else {
        Connection conn = obj.getConnection();
        String sql = "SELECT clientName FROM inodeuc WHERE id = ?;";
        PreparedStatement pst = conn.prepareStatement(sql);
        pst.setLong(1, id);
        ResultSet rs = pst.executeQuery();
        while (rs.next()) {
          name = rs.getString(1);
        }
        rs.close();
        pst.close();
      }
      Database.getInstance().retConnection(obj);
    } catch (SQLException ex) {
      System.err.println(ex.getMessage());
    }
    if (LOG.isInfoEnabled()) {
      LOG.info("getUcClientName [GET]: (" + id + ", " + name + ")");
    }
    return name;
  }

  public static void setUcClientName(final long id, final String clientName) {
    try {
      DatabaseConnection obj = Database.getInstance().getConnection();
      Connection conn = obj.getConnection();
      String sql = "UPDATE inodeuc SET clientName = ? WHERE id = ?;";
      PreparedStatement pst = conn.prepareStatement(sql);
      pst.setString(1, clientName);
      pst.setLong(2, id);
      pst.executeUpdate();
      pst.close();
      Database.getInstance().retConnection(obj);
    } catch (SQLException ex) {
      System.err.println(ex.getMessage());
    }
    if (LOG.isInfoEnabled()) {
      LOG.info("setUcClientName [UPDATE]: (" + id + ", " + clientName + ")");
    }
  }

  public static String getUcClientMachine(final long id) {
    String name = null;
    try {
      DatabaseConnection obj = Database.getInstance().getConnection();
      Connection conn = obj.getConnection();
      String sql = "SELECT clientMachine FROM inodeuc WHERE id = ?;";
      PreparedStatement pst = conn.prepareStatement(sql);
      pst.setLong(1, id);
      ResultSet rs = pst.executeQuery();
      while (rs.next()) {
        name = rs.getString(1);
      }
      rs.close();
      pst.close();
      Database.getInstance().retConnection(obj);
    } catch (SQLException ex) {
      System.err.println(ex.getMessage());
    }
    if (LOG.isInfoEnabled()) {
      LOG.info("getUcClientMachine [GET]: (" + id + ", " + name + ")");
    }
    return name;
  }

  public static void setUcClientMachine(final long id, final String clientMachine) {
    try {
      DatabaseConnection obj = Database.getInstance().getConnection();
      Connection conn = obj.getConnection();
      String sql = "UPDATE inodeuc SET clientMachine = ? WHERE id = ?;";
      PreparedStatement pst = conn.prepareStatement(sql);
      pst.setString(1, clientMachine);
      pst.setLong(2, id);
      pst.executeUpdate();
      pst.close();
    } catch (SQLException ex) {
      System.err.println(ex.getMessage());
    }
    if (LOG.isInfoEnabled()) {
      LOG.info("setUcClientMachine [UPDATE]: (" + id + ", " + clientMachine + ")");
    }
  }

  public static void removeINodeNoRecursive(final long id) {
    try {
      DatabaseConnection obj = Database.getInstance().getConnection();
      String env = System.getenv("DATABASE");
      if (env.equals("VOLT")) {
        // call a stored procedure
        try {
          obj.getVoltClient().callProcedure(new NullCallback(), "RemoveINodeNoRecursive", id);
        } catch (Exception e) {
          e.printStackTrace();
        }
      } else {
        Connection conn = obj.getConnection();
        // delete file/directory
        String sql = "DELETE FROM inodes WHERE id = ?;";
        PreparedStatement pst = conn.prepareStatement(sql);
        pst.setLong(1, id);
        pst.executeUpdate();
        pst.close();
      }
      Database.getInstance().retConnection(obj);
    } catch (SQLException ex) {
      System.err.println(ex.getMessage());
    }
    if (LOG.isInfoEnabled()) {
      LOG.info("removeINodeNoRecursive: " + id);
    }
  }

  public static void removeUc(final long id) {
    try {
      DatabaseConnection obj = Database.getInstance().getConnection();
      Connection conn = obj.getConnection();
      String sql = "DELETE FROM inodeuc WHERE id = ?;";
      PreparedStatement pst = conn.prepareStatement(sql);
      pst.setLong(1, id);
      pst.executeUpdate();
      pst.close();
      Database.getInstance().retConnection(obj);
    } catch (SQLException ex) {
      System.err.println(ex.getMessage());
    }
    if (LOG.isInfoEnabled()) {
      LOG.info("removeUc [UPDATE]: (" + id + ")");
    }
  }

  public static String getXAttrValue(final long id) {
    String value = null;
    try {
      DatabaseConnection obj = Database.getInstance().getConnection();
      Connection conn = obj.getConnection();
      String sql = "SELECT value FROM inodexattrs WHERE id = ?;";
      PreparedStatement pst = conn.prepareStatement(sql);
      pst.setLong(1, id);
      ResultSet rs = pst.executeQuery();
      while (rs.next()) {
        value = rs.getString(1);
      }
      rs.close();
      pst.close();
      Database.getInstance().retConnection(obj);
    } catch (SQLException ex) {
      System.err.println(ex.getMessage());
    }
    if (LOG.isInfoEnabled()) {
      LOG.info("getXAttrValue [GET]: (" + id + ", " + value + ")");
    }
    return value;
  }

  public static String getXAttrName(final long id) {
    String name = null;
    try {
      DatabaseConnection obj = Database.getInstance().getConnection();
      Connection conn = obj.getConnection();
      String sql = "SELECT name FROM inodexattrs WHERE id = ?;";
      PreparedStatement pst = conn.prepareStatement(sql);
      pst.setLong(1, id);
      ResultSet rs = pst.executeQuery();
      while (rs.next()) {
        name = rs.getString(1);
      }
      rs.close();
      pst.close();
      Database.getInstance().retConnection(obj);
    } catch (SQLException ex) {
      System.err.println(ex.getMessage());
    }
    if (LOG.isInfoEnabled()) {
      LOG.info("getXAttrName [GET]: (" + id + ", " + name + ")");
    }
    return name;
  }

  public static int getXAttrNameSpace(final long id) {
    int ns = -1;
    try {
      DatabaseConnection obj = Database.getInstance().getConnection();
      Connection conn = obj.getConnection();
      String sql = "SELECT namespace FROM inodexattrs WHERE id = ?;";
      PreparedStatement pst = conn.prepareStatement(sql);
      pst.setLong(1, id);
      ResultSet rs = pst.executeQuery();
      while (rs.next()) {
        ns = rs.getInt(1);
      }
      rs.close();
      pst.close();
      Database.getInstance().retConnection(obj);
    } catch (SQLException ex) {
      System.err.println(ex.getMessage());
    }
    if (LOG.isInfoEnabled()) {
      LOG.info("getXAttrNameSpace [GET]: (" + id + ", " + ns + ")");
    }
    return ns;
  }

  public class XAttrInfo {
    public int namespace;
    public String name;
    public String value;

    public XAttrInfo(int ns, String name, String val) {
      this.namespace = ns;
      this.name = name;
      this.value = val;
    }

    public int getNameSpace() {
      return namespace;
    }

    public String getName() {
      return name;
    }

    public String getValue() {
      return value;
    }
  }

  public List<XAttrInfo> getXAttrs(final long id) {
    List<XAttrInfo> xinfo = new ArrayList<XAttrInfo>();
    try {
      DatabaseConnection obj = Database.getInstance().getConnection();
      Connection conn = obj.getConnection();
      String sql = "SELECT namespace, name, value FROM inodexattrs WHERE id = ?;";
      PreparedStatement pst = conn.prepareStatement(sql);
      pst.setLong(1, id);
      ResultSet rs = pst.executeQuery();
      while (rs.next()) {
        xinfo.add(new XAttrInfo(rs.getInt(1), rs.getString(2), rs.getString(3)));
      }
      rs.close();
      pst.close();
      Database.getInstance().retConnection(obj);
    } catch (SQLException ex) {
      System.err.println(ex.getMessage());
    }
    if (LOG.isInfoEnabled()) {
      LOG.info("getXAttrs [GET]: (" + id + ")");
    }
    return xinfo;
  }

  public static Boolean checkXAttrExistence(final long id) {
    boolean exist = false;
    try {
      DatabaseConnection obj = Database.getInstance().getConnection();
      String env = System.getenv("DATABASE");
      if (env.equals("VOLT")) {
        try {
          VoltTable[] results =
              obj.getVoltClient().callProcedure("CheckXAttrExistence", id).getResults();
          VoltTable result = results[0];
          result.resetRowPosition();
          while (result.advanceRow()) {
            if (result.getLong(0) >= 1) {
              exist = true;
            }
          }
        } catch (Exception e) {
          e.printStackTrace();
        }
      } else {
        Connection conn = obj.getConnection();
        String sql = "SELECT COUNT(id) FROM inodexattrs WHERE id = ?;";
        PreparedStatement pst = conn.prepareStatement(sql);
        pst.setLong(1, id);
        ResultSet rs = pst.executeQuery();
        while (rs.next()) {
          if (rs.getInt(1) >= 1) {
            exist = true;
          }
        }
        rs.close();
        pst.close();
      }
      Database.getInstance().retConnection(obj);
    } catch (SQLException ex) {
      System.err.println(ex.getMessage());
    }
    if (LOG.isInfoEnabled()) {
      LOG.info("checkXAttrExistence [GET]: (" + id + ", " + exist + ")");
    }
    return exist;
  }

  public static void insertXAttr(
      final long id, final int namespace, final String name, final String value) {
    try {
      DatabaseConnection obj = Database.getInstance().getConnection();
      String env = System.getenv("DATABASE");
      if (env.equals("VOLT")) {
        try {
          obj.getVoltClient()
              .callProcedure(new NullCallback(), "InsertXAttr", id, namespace, name, value);
        } catch (Exception e) {
          e.printStackTrace();
        }
      } else {
        Connection conn = obj.getConnection();
        String sql = "INSERT INTO inodexattrs(id, namespace, name, value) VALUES (?, ?, ?, ?);";
        PreparedStatement pst = conn.prepareStatement(sql);
        pst.setLong(1, id);
        pst.setInt(2, namespace);
        pst.setString(3, name);
        pst.setString(4, value);
        pst.executeUpdate();
        pst.close();
      }
      Database.getInstance().retConnection(obj);
    } catch (SQLException ex) {
      System.err.println(ex.getMessage());
    }
    if (LOG.isInfoEnabled()) {
      LOG.info(
          "insertXAttr [UPDATE]: (" + id + ", " + namespace + ", " + name + ", " + value + ")");
    }
  }

  public static void removeXAttr(final long id) {
    try {
      DatabaseConnection obj = Database.getInstance().getConnection();
      Connection conn = obj.getConnection();
      String sql = "DELETE FROM inodexattrs WHERE id = ?;";
      PreparedStatement pst = conn.prepareStatement(sql);
      pst.setLong(1, id);
      pst.executeUpdate();
      pst.close();
      Database.getInstance().retConnection(obj);
    } catch (SQLException ex) {
      System.err.println(ex.getMessage());
    }
    if (LOG.isInfoEnabled()) {
      LOG.info("removeXAttr [UPDATE]: (" + id + ")");
    }
  }

  public static void insertXAttrs(
      final long id, final List<Integer> ns, final List<String> namevals) {
    try {
      String env = System.getenv("DATABASE");
      if (env.equals("VOLT")) {
        // call a stored procedure
        DatabaseConnection obj = Database.getInstance().getConnection();
        Connection conn = obj.getConnection();
        CallableStatement proc = conn.prepareCall("{call InsertXAttrs(?, ?, ?)}");
        proc.setLong(1, id);
        proc.setArray(2, conn.createArrayOf("SMALLINT", ns.toArray(new Long[ns.size()])));
        proc.setArray(
            3, conn.createArrayOf("VARCHAR", namevals.toArray(new String[namevals.size()])));
        ResultSet rs = proc.executeQuery();
        while (rs.next()) {
          if (LOG.isInfoEnabled()) {
            LOG.info("insertXAttrs Return: " + rs.getLong(1));
          }
        }
        rs.close();
        proc.close();
        Database.getInstance().retConnection(obj);
      } else {
        DatabaseConnection obj = Database.getInstance().getConnection();
        Connection conn = obj.getConnection();
        String sql = "";
        for (int i = 0; i < ns.size(); ++i) {
          sql += "INSERT INTO inodexattrs(id, namespace, name, value) VALUES(?, ?, ?, ?);";
        }
        PreparedStatement pst = conn.prepareStatement(sql);
        for (int i = 0; i < ns.size(); ++i) {
          pst.setLong(i * 4 + 1, id);
          pst.setInt(i * 4 + 2, ns.get(i));
          pst.setString(i * 4 + 3, namevals.get(i * 2));
          pst.setString(i * 4 + 4, namevals.get(i * 2 + 1));
        }
        pst.executeUpdate();
        pst.close();
        Database.getInstance().retConnection(obj);
      }
    } catch (SQLException ex) {
      System.err.println(ex.getMessage());
    }
    if (LOG.isInfoEnabled()) {
      LOG.info("insertXAttrs: " + id);
    }
  }

  public static void batchRemoveINodes(final List<Long> ids) throws SQLException {
    try {
      DatabaseConnection obj = Database.getInstance().getConnection();
      String env = System.getenv("DATABASE");
      if (env.equals("VOLT")) {
        try {
          long[] list = ids.stream().mapToLong(l -> l).toArray();
          obj.getVoltClient().callProcedure("BatchRemoveINodes", list);
        } catch (Exception e) {
          e.printStackTrace();
        }
      } else {
        // Connection conn = obj.getConnection();
        // PreparedStatement pst = conn.prepareStatement(sql);
        // pst.setLong(1, childId);
        // pst.executeUpdate();
        // pst.close();
        // TODO: Support batch update in CockroachDB
        throw new SQLException("[UNSUPPORT] Invalid operation ...");
      }
      Database.getInstance().retConnection(obj);
    } catch (SQLException ex) {
      System.err.println(ex.getMessage());
    }
    if (LOG.isInfoEnabled()) {
      LOG.info("batchRemoveINodes [UPDATE]");
    }
  }

  public static void batchUpdateINodes(
      final List<Long> longAttr,
      final List<String> strAttr,
      final List<Long> fileIds,
      final List<String> fileAttr)
      throws SQLException {
    try {
      DatabaseConnection obj = Database.getInstance().getConnection();
      String env = System.getenv("DATABASE");
      if (env.equals("VOLT")) {
        try {
          obj.getVoltClient()
              .callProcedure(
                  "BatchUpdateINodes",
                  longAttr.toArray(new Long[longAttr.size()]),
                  strAttr.toArray(new String[strAttr.size()]),
                  fileIds.toArray(new Long[fileIds.size()]),
                  fileAttr.toArray(new String[fileAttr.size()]));
        } catch (Exception e) {
          e.printStackTrace();
        }
      } else {
        // Connection conn = obj.getConnection();
        // PreparedStatement pst = conn.prepareStatement(sql);
        // pst.setLong(1, childId);
        // pst.executeUpdate();
        // pst.close();
        // TODO: Support batch update in CockroachDB
        throw new SQLException("[UNSUPPORT] Invalid operation ...");
      }
      Database.getInstance().retConnection(obj);
    } catch (SQLException ex) {
      System.err.println(ex.getMessage());
    }
    if (LOG.isInfoEnabled()) {
      LOG.info("batchUpdateINodes [UPDATE]");
    }
  }

  public static void updateSubtree(final long dir_id, final long dest_id, final String old_parent_name,
    final String new_parent_name, final long new_parent) {
    try {
      DatabaseConnection obj = Database.getInstance().getConnection();
      String env = System.getenv("DATABASE");
      if (env.equals("VOLT")) {
        try {
          obj.getVoltClient()
              .callProcedure("UpdateSubtree", dir_id, dest_id, old_parent_name,
              new_parent_name, new_parent);
        } catch (Exception e) {
          e.printStackTrace();
        }
      } else {
        throw new SQLException("[UNSUPPORT] Invalid operation ...");
      }
      Database.getInstance().retConnection(obj);
    } catch (SQLException ex) {
      System.err.println(ex.getMessage());
    }
    if (LOG.isInfoEnabled()) {
      LOG.info("updateSubtree [UPDATE]: " + dir_id);
    }
  }

  public static void setId(final long old_id, final long new_id, final String new_parent_name, final long new_parent) {
    try {
      DatabaseConnection obj = Database.getInstance().getConnection();
      String env = System.getenv("DATABASE");
      if (env.equals("VOLT")) {
        try {
          obj.getVoltClient()
              .callProcedure("SetId", old_id, new_id, new_parent_name, new_parent);
        } catch (Exception e) {
          e.printStackTrace();
        }
      } else {
        throw new SQLException("[UNSUPPORT] Invalid operation ...");
      }
      Database.getInstance().retConnection(obj);
    } catch (SQLException ex) {
      System.err.println(ex.getMessage());
    }
    if (LOG.isInfoEnabled()) {
      LOG.info("setId [UPDATE]: (" + old_id + ", " + new_id + ")");
    }
  }
}
