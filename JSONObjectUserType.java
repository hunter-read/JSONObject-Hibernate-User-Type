import java.io.Serializable;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.HibernateException;
import org.hibernate.usertype.UserType;

import org.json.JSONException;
import org.json.JSONObject;

/**
 * This class is an implementation of UserType for org.json.JSONObject
 * Supports Hibernate 5.2 and 5.3
 *
 * From the UserType Interface:
 * A "type" class is <em>not</em> the actual property type - it
 * is a class that knows how to serialize instances of another
 * class to and from JDBC.<br>
 *
 * @see org.json.JSONObject
 * @see org.hibernate.usertype.UserType
 * @author Hunter Read
 */
public class JSONObjectUserType implements UserType {

  /**
   * Return the SQL type codes for the columns mapped by this type. The
   * codes are defined on <tt>java.sql.Types</tt>.
   * @see java.sql.Types
   * @return int[] the typecodes
   */
  @Override
  public int[] sqlTypes() {
    return new int[]{Types.JAVA_OBJECT};
  }

  /**
   * The class returned by <tt>nullSafeGet()</tt>.
   *
   * @return Class
   */
  @Override
  public Class returnedClass() {
    return JSONObject.class;
  }

  /**
   * Compare two instances of the class mapped by this type for persistence "equality".
   * Equality of the persistent state.
   *
   * @param x
   * @param y
   * @return boolean
   */
  @Override
  public boolean equals(Object x, Object y) throws HibernateException {
    if (x == null) return (y != null);
    return (x.equals(y));
  }

  /**
   * Get a hashcode for the instance, consistent with persistence "equality"
   */
  @Override
  public int hashCode(Object x) throws HibernateException {
    if (x == null) {
      return 0;
    }
    return x.hashCode();
  }

  /**
   * Retrieve an instance of the mapped class from a JDBC resultset. Implementors
   * should handle possibility of null values.
   *
   *
   * @param rs a JDBC result set
   * @param names the column names
   * @param session
   * @param owner the containing entity  @return Object
   * @throws HibernateException
   * @throws SQLException
   */
  @Override
  public Object nullSafeGet(ResultSet rs, String[] names, SharedSessionContractImplementor session, Object owner) throws HibernateException, SQLException {
    if (rs.getObject(names[0]) != null) {
      try {
        return new JSONObject(rs.getString(names[0]));
      } catch (JSONException e) {
        throw new RuntimeException(e);
      }
    }
    return null;
  }

  /**
   * Write an instance of the mapped class to a prepared statement. Implementors
   * should handle possibility of null values. A multi-column type should be written
   * to parameters starting from <tt>index</tt>.
   *
   *
   * @param st a JDBC prepared statement
   * @param value the object to write
   * @param index statement parameter index
   * @param session
   * @throws HibernateException
   * @throws SQLException
   */
  @Override
  public void nullSafeSet(PreparedStatement st, Object value, int index, SharedSessionContractImplementor session) throws HibernateException, SQLException {
    if (value == null) {
      st.setNull(index, Types.OTHER);
    } else {
      st.setObject(index, ((JSONObject) value).toString(), Types.OTHER);
    }
  }

  /**
   * Return a deep copy of the persistent state, stopping at entities and at
   * collections. It is not necessary to copy immutable objects, or null
   * values, in which case it is safe to simply return the argument.
   *
   * @param value the object to be cloned, which may be null
   * @return Object a copy
   */
  @Override
  public Object deepCopy(Object value) throws HibernateException {
    if (value == null) return value;
    try {
      return new JSONObject(((JSONObject) value).toString());
    } catch (JSONException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Are objects of this type mutable?
   *
   * @return boolean
   */
  @Override
  public boolean isMutable() {
    return true;
  }

  /**
   * Transform the object into its cacheable representation. At the very least this
   * method should perform a deep copy if the type is mutable. That may not be enough
   * for some implementations, however; for example, associations must be cached as
   * identifier values. (optional operation)
   *
   * @param value the object to be cached
   * @return a cachable representation of the object
   * @throws HibernateException
   */
  @Override
  public Serializable disassemble(Object value) throws HibernateException {
    return ((JSONObject) value).toString();
  }

  /**
   * Reconstruct an object from the cacheable representation. At the very least this
   * method should perform a deep copy if the type is mutable. (optional operation)
   *
   * @param cached the object to be cached
   * @param owner the owner of the cached object
   * @return a reconstructed object from the cachable representation
   * @throws HibernateException
   */
  @Override
  public Object assemble(Serializable cached, Object owner) throws HibernateException {
    return deepCopy(cached);
  }

  /**
   * During merge, replace the existing (target) value in the entity we are merging to
   * with a new (original) value from the detached entity we are merging. For immutable
   * objects, or null values, it is safe to simply return the first parameter. For
   * mutable objects, it is safe to return a copy of the first parameter. For objects
   * with component values, it might make sense to recursively replace component values.
   *
   * @param original the value from the detached entity being merged
   * @param target the value in the managed entity
   * @return the value to be merged
   */
  @Override
  public Object replace(Object original, Object target, Object owner) throws HibernateException {
    return deepCopy(original);
  }
}
