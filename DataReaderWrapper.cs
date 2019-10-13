using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using SqlTypes = System.Data.SqlTypes;

namespace KG.System.Data.SqlClient.Extensions.ReaderWrapper
{
    /// <summary>
    /// Helper class for reading from data reader.
    /// </summary>
    public class DataReaderWrapper : IDisposable
    {
        /// <summary>
        /// The reader being wrapper
        /// </summary>
        private IDataReader reader;

        /// <summary>
        /// Internal dictionary to cache the ordinal positions of columns
        /// </summary>
        private Dictionary<string, int> keys;

        public string[] FieldNames { get; private set; }

        #region Constructors
        /// <summary>
        /// Initializes a new instance of the DataReaderWrapper class.
        /// </summary>
        /// <param name="reader">The DataReader object to wrap</param>
        /// <param name="description">The external resource being read, for example name of stored procedure or resultset being read.  Used for logging and error messages.</param>
        public DataReaderWrapper(IDataReader reader, string description)
        {
            this.reader = reader;
            this.Description = description;
            this.keys = new Dictionary<string, int>();
            this.MakeOrdinalsTable();
        }
        #endregion //Constructors

        /// <summary>
        /// A delegate used by a generic method that reads nullable types from a datareader
        /// </summary>
        /// <typeparam name="T">The Value type being returned</typeparam>
        /// <param name="ordinal">Number of the field/column to find.</param>
        /// <returns>A nullable type of T containing the value being read.</returns>
        private delegate T ConversionInt<T>(int ordinal);

        #region Business Properties
        /// <summary>
        /// Gets or sets a description of the usage of the reader, for example the external resource being read, name of stored procedure or the current resultset.
        /// </summary>
        /// <remarks>Used for logging and error messages.</remarks>
        /// <value>A text description of the usage of the reader.</value>
        public string Description { get; set; }

        /// <summary>
        /// Gets a value indicating whether the data reader is closed.
        /// </summary>
        /// <value>true if the data reader is closed; otherwise, false.</value>
        public bool IsClosed => this.reader.IsClosed;

        /// <summary>
        /// Gets a value indicating the depth of nesting for the current row.
        /// </summary>
        /// <value>The level of nesting.</value>
        public int Depth => this.reader.Depth;

        /// <summary>
        /// Gets the number of columns in the current row.
        /// </summary>
        /// <value>When not positioned in a valid recordset, 0; otherwise the number of columns in the current row. The default is -1.</value>
        public int FieldCount => this.reader.FieldCount;

        /// <summary>
        /// Gets the number of rows changed, inserted, or deleted by execution of the SQL statement.
        /// </summary>
        /// <value>The number of rows changed, inserted, or deleted; 0 if no rows were affected or the statement failed; and -1 for SELECT statements.</value>
        public int RecordsAffected => this.reader.RecordsAffected;

        #endregion //Business Properties

        #region Business Methods
        /// <summary>
        /// Advances the System.IDataReader to the next record.
        /// </summary>
        /// <returns>true if there are more rows; otherwise, false.</returns>
        public bool Read()
        {
            return this.reader.Read();
        }

        /// <summary>
        /// Closes the System.Data.IDataReader Object.
        /// </summary>
        public void Close()
        {
            this.reader.Close();
        }

        /* 
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1024:UsePropertiesWhereAppropriate", Justification = "n/a")]
        public DataTable GetSchemaTable()
        {
            return _reader.GetSchemaTable();
        }
        */

        /// <summary>
        /// Advances the data reader to the next result, when reading the results of batch SQL statements.
        /// </summary>
        /// <returns>true if there are more rows; otherwise, false.</returns>
        public bool NextResult()
        {
            bool nextResult = this.reader.NextResult();

            if (nextResult == true)
            {
                this.MakeOrdinalsTable();
            }

            return nextResult;
        }

        /// <summary>
        /// Return the index of the named field.
        /// </summary>
        /// <remarks>Supports case-insensitive search for the field</remarks>
        /// <param name="name">The name of the field to find.</param>
        /// <returns>The index of the named field.  -1 if field is not found.</returns>
        public int GetOrdinal(string name)
        {
            if (name == null)
            {
                throw new ArgumentNullException("name");
            }

            int ordinal = -1;

            if (this.keys.TryGetValue(name, out ordinal) == true)
            {
                return ordinal;
            }
            else
            {
                ordinal = -1;
            }

            // Try a case-insensitive search for the field
            for (int i = 0; i < this.FieldNames.Length; i++)
            {
                if (CultureInfo.CurrentCulture.CompareInfo.Compare(name, this.FieldNames[i], CompareOptions.IgnoreCase) == 0)
                {
                    this.keys[name] = i;
                    ordinal = i;
                    break;
                }
            }

            // If the field isn't found, cast an exception so this behaves the same way as a normal datareader and the developer gets the fieldName that wasn't found.
            if (ordinal == -1)
            {
                // throw new IndexOutOfRangeException(fieldName);
                throw new ArgumentOutOfRangeException(name);
            }

            return ordinal;
        }

        /// <summary>
        /// Determines whether the reader contains a specific field.
        /// </summary>
        /// <param name="field">Name of the field</param>
        /// <returns>
        ///   <c>true</c> if the reader has field with given name; otherwise, <c>false</c>.
        /// </returns>
        public bool HasField(string field)
        {
            return this.keys.ContainsKey(field);
        }

        /// <summary>
        /// Return whether the specified field is set to null.
        /// </summary>
        /// <param name="name">The name of the field to find.</param>
        /// <returns>true if the specified field is set to null. Otherwise, false.</returns>
        public bool IsDBNull(string name)
        {
            return this.reader.IsDBNull(this.GetOrdinal(name));
        }

        /// <summary>
        /// Gets the data type information for the specified field.
        /// </summary>
        /// <param name="name">The name of the field to find.</param>
        /// <returns>The data type information for the specified field.</returns>
        public string GetDataTypeName(string name)
        {
            return this.reader.GetDataTypeName(this.GetOrdinal(name));
        }

        /// <summary>
        /// Gets the System.Type information corresponding to the type of System.Object that would be returned from System.Data.IDataRecord.GetValue(System.Int32).
        /// </summary>
        /// <param name="name">The name of the field to find.</param>
        /// <returns>The <see cref="T:System.Type"></see> information corresponding to the type of <see cref="T:System.Object"></see> that would be returned from <see cref="M:System.Data.IDataRecord.GetValue(System.Int32)"></see>.</returns>
        public Type GetFieldType(string name)
        {
            return this.reader.GetFieldType(this.GetOrdinal(name));
        }

        #region GetX(string)
        /// <summary>
        /// Reads a boolean value from the specified field name.
        /// </summary>
        /// <remarks>If the read returns a DBNull the default value for the type being read is returned</remarks>
        /// <param name="name">The name of the field to read.</param>
        /// <returns>The value of the field.</returns>
        public bool GetBoolean(string name)
        {
            return this.GetBoolean(name, default(bool));
        }

        /// <summary>
        /// Gets the 8-bit unsigned integer value of the specified column.
        /// </summary>
        /// <param name="name">The name of the field to read.</param>
        /// <returns>The 8-bit unsigned integer value of the specified column.</returns>
        public byte GetByte(string name)
        {
            return this.GetByte(name, default(byte));
        }

        /// <summary>
        /// Gets the character value of the specified column.
        /// </summary>
        /// <param name="name">The name of the field to read.</param>
        /// <returns>The character value of the specified column.</returns>
        public char GetChar(string name)
        {
            return this.GetChar(name, default(char));
        }

        /// <summary>
        /// Gets the date and time data value of the specified field.
        /// </summary>
        /// <param name="name">The name of the field to read.</param>
        /// <returns>The date and time data value of the specified field.</returns>
        public DateTime GetDateTime(string name)
        {
            return this.GetDateTime(name, default(DateTime));
        }

        /// <summary>
        /// Gets the fixed-position numeric value of the specified field.
        /// </summary>
        /// <param name="name">The name of the field to read.</param>
        /// <returns>The fixed-position numeric value of the specified field.</returns>
        public decimal GetDecimal(string name)
        {
            return this.GetDecimal(name, default(decimal));
        }

        /// <summary>
        /// Gets the double-precision floating point number of the specified field.
        /// </summary>
        /// <param name="name">The name of the field to read.</param>
        /// <returns>The double-precision floating point number of the specified field.</returns>
        public double GetDouble(string name)
        {
            return this.GetDouble(name, default(double));
        }

        /// <summary>
        /// Gets the single-precision floating point number of the specified field.
        /// </summary>
        /// <param name="name">The name of the field to read.</param>
        /// <returns>The single-precision floating point number of the specified field.</returns>
        [SuppressMessage("Microsoft.Naming", "CA1720:IdentifiersShouldNotContainTypeNames", MessageId = "float", Justification = "Original data reader already has a method with this name.")]
        public float GetFloat(string name)
        {
            return this.GetFloat(name, default(float));
        }

        /// <summary>
        /// Returns the GUID value of the specified field.
        /// </summary>
        /// <param name="name">The name of the field to read.</param>
        /// <returns>The GUID value of the specified field.</returns>
        public Guid GetGuid(string name)
        {
            return this.GetGuid(name, default(Guid));
        }

        /// <summary>
        /// Gets the 16-bit signed integer value of the specified field.
        /// </summary>
        /// <param name="name">The name of the field to read.</param>
        /// <returns>The 16-bit signed integer value of the specified field.</returns>
        public short GetInt16(string name)
        {
            return this.GetInt16(name, default(short));
        }

        /// <summary>
        /// Gets the 32-bit signed integer value of the specified field.
        /// </summary>
        /// <param name="name">The name of the field to read.</param>
        /// <returns>The 32-bit signed integer value of the specified field.</returns>
        public int GetInt32(string name)
        {
            return this.GetInt32(name, default(int));
        }

        /// <summary>
        /// Gets the 64-bit signed integer value of the specified field.
        /// </summary>
        /// <param name="name">The name of the field to read.</param>
        /// <returns>The 64-bit signed integer value of the specified field.</returns>
        public long GetInt64(string name)
        {
            return this.GetInt64(name, default(long));
        }

        /// <summary>
        /// Gets the string value of the specified field.
        /// </summary>
        /// <param name="name">The name of the field to read.</param>
        /// <returns>The string value of the specified field.</returns>
        public string GetString(string name)
        {
            return this.GetString(name, string.Empty);
        }
        #endregion

        #region GetX(string, defaultNullValue)
        /// <summary>
        /// Reads a boolean value from the specified field name.
        /// </summary>
        /// <param name="name">The name of the field to read.</param>
        /// <param name="defaultNullValue">The value returned if the specified field contains a null value.</param>
        /// <returns>The value of the field, or defaultNullValue if the field contains null.</returns>
        public bool GetBoolean(string name, bool defaultNullValue)
        {
            if (this.reader.IsDBNull(this.GetOrdinal(name)) == true)
            {
                return defaultNullValue;
            }
            else
            {
                try
                {
                    return this.reader.GetBoolean(this.GetOrdinal(name));
                }
                catch (InvalidCastException ex)
                {
                    throw new InvalidCastException("Invalid cast reading field '" + name + "', Reader: '" + this.Description + "'", ex);
                }
            }
        }

        /// <summary>
        /// Gets the 8-bit unsigned integer value of the specified column.
        /// </summary>
        /// <param name="name">The name of the field to read.</param>
        /// <param name="defaultNullValue">The value returned if the specified field contains a null value.</param>
        /// <returns>The 8-bit unsigned integer value of the specified column, or defaultNullValue if the field contains null.</returns>
        public byte GetByte(string name, byte defaultNullValue)
        {
            if (this.reader.IsDBNull(this.GetOrdinal(name)) == true)
            {
                return defaultNullValue;
            }
            else
            {
                try
                {
                    return this.reader.GetByte(this.GetOrdinal(name));
                }
                catch (InvalidCastException ex)
                {
                    throw new InvalidCastException("Invalid cast reading field '" + name + "', Reader: '" + this.Description + "'", ex);
                }
            }
        }

        /// <summary>
        /// Gets the character value of the specified column.
        /// </summary>
        /// <param name="name">The name of the field to read.</param>
        /// <param name="defaultNullValue">The value returned if the specified field contains a null value.</param>
        /// <returns>The character value of the specified column, or defaultNullValue if the field contains null.</returns>
        public char GetChar(string name, char defaultNullValue)
        {
            if (this.reader.IsDBNull(this.GetOrdinal(name)) == true)
            {
                return defaultNullValue;
            }
            else
            {
                try
                {
                    // Not supported in SqlClient and OleDb so we roll our own
                    string temp = this.reader.GetString(this.GetOrdinal(name)).Trim();

                    if (temp.Length > 0)
                    {
                        return temp[0];
                    }
                    else
                    {
                        return defaultNullValue;
                    }
                }
                catch (InvalidCastException ex)
                {
                    throw new InvalidCastException("Invalid cast reading field '" + name + "', Reader: '" + this.Description + "'", ex);
                }
            }
        }

        /// <summary>
        /// Gets the date and time data value of the specified field.
        /// </summary>
        /// <param name="name">The name of the field to read.</param>
        /// <param name="defaultNullValue">The value returned if the specified field contains a null value.</param>
        /// <returns>The date and time data value of the specified field, or defaultNullValue if the field contains null.</returns>
        public DateTime GetDateTime(string name, DateTime defaultNullValue)
        {
            if (this.reader.IsDBNull(this.GetOrdinal(name)) == true)
            {
                return defaultNullValue;
            }
            else
            {
                try
                {
                    return this.reader.GetDateTime(this.GetOrdinal(name));
                }
                catch (InvalidCastException ex)
                {
                    throw new InvalidCastException("Invalid cast reading field '" + name + "', Reader: '" + this.Description + "'", ex);
                }
            }
        }

        /// <summary>
        /// Gets the fixed-position numeric value of the specified field.
        /// </summary>
        /// <param name="name">The name of the field to read.</param>
        /// <param name="defaultNullValue">The value returned if the specified field contains a null value.</param>
        /// <returns>The fixed-position numeric value of the specified field, or defaultNullValue if the field contains null.</returns>
        public decimal GetDecimal(string name, decimal defaultNullValue)
        {
            if (this.reader.IsDBNull(this.GetOrdinal(name)) == true)
            {
                return defaultNullValue;
            }
            else
            {
                try
                {
                    return this.reader.GetDecimal(this.GetOrdinal(name));
                }
                catch (InvalidCastException ex)
                {
                    throw new InvalidCastException("Invalid cast reading field '" + name + "', Reader: '" + this.Description + "'", ex);
                }
            }
        }

        /// <summary>
        /// Gets the double-precision floating point number of the specified field.
        /// </summary>
        /// <param name="name">The name of the field to read.</param>
        /// <param name="defaultNullValue">The value returned if the specified field contains a null value.</param>
        /// <returns>The double-precision floating point number of the specified field, or defaultNullValue if the field contains null.</returns>
        public double GetDouble(string name, double defaultNullValue)
        {
            if (this.reader.IsDBNull(this.GetOrdinal(name)) == true)
            {
                return defaultNullValue;
            }
            else
            {
                try
                {
                    return this.reader.GetDouble(this.GetOrdinal(name));
                }
                catch (InvalidCastException ex)
                {
                    throw new InvalidCastException("Invalid cast reading field '" + name + "', Reader: '" + this.Description + "'", ex);
                }
            }
        }

        /// <summary>
        /// Gets the single-precision floating point number of the specified field.
        /// </summary>
        /// <param name="name">The name of the field to read.</param>
        /// <param name="defaultNullValue">The value returned if the specified field contains a null value.</param>
        /// <returns>The single-precision floating point number of the specified field, or defaultNullValue if the field contains null.</returns>
        [SuppressMessage("Microsoft.Naming", "CA1720:IdentifiersShouldNotContainTypeNames", MessageId = "float", Justification = "Original data reader already has a method with this name.")]
        public float GetFloat(string name, float defaultNullValue)
        {
            if (this.reader.IsDBNull(this.GetOrdinal(name)) == true)
            {
                return defaultNullValue;
            }
            else
            {
                try
                {
                    return this.reader.GetFloat(this.GetOrdinal(name));
                }
                catch (InvalidCastException ex)
                {
                    throw new InvalidCastException("Invalid cast reading field '" + name + "', Reader: '" + this.Description + "'", ex);
                }
            }
        }

        /// <summary>
        /// Returns the GUID value of the specified field.
        /// </summary>
        /// <param name="name">The name of the field to read.</param>
        /// <param name="defaultNullValue">The value returned if the specified field contains a null value.</param>
        /// <returns>The GUID value of the specified field, or defaultNullValue if the field contains null.</returns>
        public Guid GetGuid(string name, Guid defaultNullValue)
        {
            if (this.reader.IsDBNull(this.GetOrdinal(name)) == true)
            {
                return defaultNullValue;
            }
            else
            {
                try
                {
                    return this.reader.GetGuid(this.GetOrdinal(name));
                }
                catch (InvalidCastException ex)
                {
                    throw new InvalidCastException("Invalid cast reading field '" + name + "', Reader: '" + this.Description + "'", ex);
                }
            }
        }

        /// <summary>
        /// Gets the 16-bit signed integer value of the specified field.
        /// </summary>
        /// <param name="name">The name of the field to read.</param>
        /// <param name="defaultNullValue">The value returned if the specified field contains a null value.</param>
        /// <returns>The 16-bit signed integer value of the specified field, or defaultNullValue if the field contains null.</returns>
        public short GetInt16(string name, short defaultNullValue)
        {
            if (this.reader.IsDBNull(this.GetOrdinal(name)) == true)
            {
                return defaultNullValue;
            }
            else
            {
                try
                {
                    return this.reader.GetInt16(this.GetOrdinal(name));
                }
                catch (InvalidCastException ex)
                {
                    throw new InvalidCastException("Invalid cast reading field '" + name + "', Reader: '" + this.Description + "'", ex);
                }
            }
        }

        /// <summary>
        /// Gets the 32-bit signed integer value of the specified field.
        /// </summary>
        /// <param name="name">The name of the field to read.</param>
        /// <param name="defaultNullValue">The value returned if the specified field contains a null value.</param>
        /// <returns>The 32-bit signed integer value of the specified field, or defaultNullValue if the field contains null.</returns>
        public int GetInt32(string name, int defaultNullValue)
        {
            if (this.reader.IsDBNull(this.GetOrdinal(name)) == true)
            {
                return defaultNullValue;
            }
            else
            {
                try
                {
                    return this.reader.GetInt32(this.GetOrdinal(name));
                }
                catch (InvalidCastException ex)
                {
                    throw new InvalidCastException("Invalid cast reading field '" + name + "', Reader: '" + this.Description + "'", ex);
                }
            }
        }

        /// <summary>
        /// Gets the 64-bit signed integer value of the specified field.
        /// </summary>
        /// <param name="name">The name of the field to read.</param>
        /// <param name="defaultNullValue">The value returned if the specified field contains a null value.</param>
        /// <returns>The 64-bit signed integer value of the specified field, or defaultNullValue if the field contains null.</returns>
        public long GetInt64(string name, long defaultNullValue)
        {
            if (this.reader.IsDBNull(this.GetOrdinal(name)) == true)
            {
                return defaultNullValue;
            }
            else
            {
                try
                {
                    return this.reader.GetInt64(this.GetOrdinal(name));
                }
                catch (InvalidCastException ex)
                {
                    throw new InvalidCastException("Invalid cast reading field '" + name + "', Reader: '" + this.Description + "'", ex);
                }
            }
        }

        /// <summary>
        /// Gets the string value of the specified field.
        /// </summary>
        /// <param name="name">The name of the field to read.</param>
        /// <param name="defaultNullValue">The value returned if the specified field contains a null value.</param>
        /// <returns>The string value of the specified field, or defaultNullValue if the field contains null.</returns>
        public string GetString(string name, string defaultNullValue)
        {
            if (this.reader.IsDBNull(this.GetOrdinal(name)) == true)
            {
                return defaultNullValue;
            }
            else
            {
                try
                {
                    return this.reader.GetString(this.GetOrdinal(name));
                }
                catch (InvalidCastException ex)
                {
                    throw new InvalidCastException("Invalid cast reading field '" + name + "', Reader: '" + this.Description + "'", ex);
                }
            }
        }
        #endregion

        #region GetXNotNull(string)
        // Þar sem isNullAllowed er í raun by default = true, þá er þetta bara notað í þeim tilfellum sem notandinn
        // vill að það komi villa ef DB skilar null.  Í þeim tilfellum er defaultNullValue GAGNSLAUST!

        /// <summary>
        /// Reads a boolean value from the specified field name.  Nulls are not allowed.
        /// </summary>
        /// <remarks>If the read returns a DBNull an exception is cast.</remarks>
        /// <param name="name">The name of the field to read.</param>
        /// <returns>The value of the field.</returns>
        public bool GetBooleanNotNull(string name)
        {
            // Will throw an exception if the value is null
            try
            {
                return this.reader.GetBoolean(this.GetOrdinal(name));
            }
            catch (InvalidCastException ex)
            {
                throw new InvalidCastException("Invalid cast reading field '" + name + "', Reader: '" + this.Description + "'", ex);
            }
            catch (SqlTypes.SqlNullValueException ex)
            {
                throw new SqlTypes.SqlNullValueException("Field " + "'" + name + "' returned an invalid null.", ex);
            }
        }

        /// <summary>
        /// Gets the 8-bit unsigned integer value of the specified column.  Nulls are not allowed.
        /// </summary>
        /// <remarks>If the read returns a DBNull an exception is cast.</remarks>
        /// <param name="name">The name of the field to read.</param>
        /// <returns>The 8-bit unsigned integer value of the specified column.</returns>
        public byte GetByteNotNull(string name)
        {
            try
            {
                return this.reader.GetByte(this.GetOrdinal(name));
            }
            catch (InvalidCastException ex)
            {
                throw new InvalidCastException("Invalid cast reading field '" + name + "', Reader: '" + this.Description + "'", ex);
            }
            catch (SqlTypes.SqlNullValueException ex)
            {
                throw new SqlTypes.SqlNullValueException("Field " + "'" + name + "' returned an invalid null.", ex);
            }
        }

        /// <summary>
        /// Gets the character value of the specified column.  Nulls are not allowed.
        /// </summary>
        /// <remarks>If the read returns a DBNull an exception is cast.</remarks>
        /// <param name="name">The name of the field to read.</param>
        /// <returns>The character value of the specified column.</returns>
        public char GetCharNotNull(string name)
        {
            try
            {
                // Not supported in SqlClient and OleDb so we roll our own
                string temp = this.reader.GetString(this.GetOrdinal(name)).Trim();
                return temp[0];
            }
            catch (InvalidCastException ex)
            {
                throw new InvalidCastException("Invalid cast reading field '" + name + "', Reader: '" + this.Description + "'", ex);
            }
            catch (SqlTypes.SqlNullValueException ex)
            {
                throw new SqlTypes.SqlNullValueException("Field " + "'" + name + "' returned an invalid null.", ex);
            }
        }

        /// <summary>
        /// Gets the date and time data value of the specified field.  Nulls are not allowed.
        /// </summary>
        /// <remarks>If the read returns a DBNull an exception is cast.</remarks>
        /// <param name="name">The name of the field to read.</param>
        /// <returns>The date and time data value of the specified field.</returns>
        public DateTime GetDateTimeNotNull(string name)
        {
            try
            {
                return this.reader.GetDateTime(this.GetOrdinal(name));
            }
            catch (InvalidCastException ex)
            {
                throw new InvalidCastException("Invalid cast reading field '" + name + "', Reader: '" + this.Description + "'", ex);
            }
            catch (SqlTypes.SqlNullValueException ex)
            {
                throw new SqlTypes.SqlNullValueException("Field " + "'" + name + "' returned an invalid null.", ex);
            }
        }

        /// <summary>
        /// Gets the fixed-position numeric value of the specified field.  Nulls are not allowed.
        /// </summary>
        /// <remarks>If the read returns a DBNull an exception is cast.</remarks>
        /// <param name="name">The name of the field to read.</param>
        /// <returns>The fixed-position numeric value of the specified field.</returns>
        public decimal GetDecimalNotNull(string name)
        {
            try
            {
                return this.reader.GetDecimal(this.GetOrdinal(name));
            }
            catch (InvalidCastException ex)
            {
                throw new InvalidCastException("Invalid cast reading field '" + name + "', Reader: '" + this.Description + "'", ex);
            }
            catch (SqlTypes.SqlNullValueException ex)
            {
                throw new SqlTypes.SqlNullValueException("Field " + "'" + name + "' returned an invalid null.", ex);
            }
        }

        /// <summary>
        /// Gets the double-precision floating point number of the specified field.  Nulls are not allowed.
        /// </summary>
        /// <remarks>If the read returns a DBNull an exception is cast.</remarks>
        /// <param name="name">The name of the field to read.</param>
        /// <returns>The double-precision floating point number of the specified field.</returns>
        public double GetDoubleNotNull(string name)
        {
            try
            {
                return this.reader.GetDouble(this.GetOrdinal(name));
            }
            catch (InvalidCastException ex)
            {
                throw new InvalidCastException("Invalid cast reading field '" + name + "', Reader: '" + this.Description + "'", ex);
            }
            catch (SqlTypes.SqlNullValueException ex)
            {
                throw new SqlTypes.SqlNullValueException("Field " + "'" + name + "' returned an invalid null.", ex);
            }
        }

        /// <summary>
        /// Gets the single-precision floating point number of the specified field.  Nulls are not allowed.
        /// </summary>
        /// <remarks>If the read returns a DBNull an exception is cast.</remarks>
        /// <param name="name">The name of the field to read.</param>
        /// <returns>The single-precision floating point number of the specified field.</returns>
        [SuppressMessage("Microsoft.Naming", "CA1720:IdentifiersShouldNotContainTypeNames", MessageId = "float", Justification = "Original data reader already has a method with this name.")]
        public float GetFloatNotNull(string name)
        {
            try
            {
                return this.reader.GetFloat(this.GetOrdinal(name));
            }
            catch (InvalidCastException ex)
            {
                throw new InvalidCastException("Invalid cast reading field '" + name + "', Reader: '" + this.Description + "'", ex);
            }
            catch (SqlTypes.SqlNullValueException ex)
            {
                throw new SqlTypes.SqlNullValueException("Field " + "'" + name + "' returned an invalid null.", ex);
            }
        }

        /// <summary>
        /// Returns the GUID value of the specified field.  Nulls are not allowed.
        /// </summary>
        /// <remarks>If the read returns a DBNull an exception is cast.</remarks>
        /// <param name="name">The name of the field to read.</param>
        /// <returns>The GUID value of the specified field.</returns>
        public Guid GetGuidNotNull(string name)
        {
            try
            {
                return this.reader.GetGuid(this.GetOrdinal(name));
            }
            catch (InvalidCastException ex)
            {
                throw new InvalidCastException("Invalid cast reading field '" + name + "', Reader: '" + this.Description + "'", ex);
            }
            catch (SqlTypes.SqlNullValueException ex)
            {
                throw new SqlTypes.SqlNullValueException("Field " + "'" + name + "' returned an invalid null.", ex);
            }
        }

        /// <summary>
        /// Gets the 16-bit signed integer value of the specified field.  Nulls are not allowed.
        /// </summary>
        /// <remarks>If the read returns a DBNull an exception is cast.</remarks>
        /// <param name="name">The name of the field to read.</param>
        /// <returns>The 16-bit signed integer value of the specified field.</returns>
        public short GetInt16NotNull(string name)
        {
            try
            {
                return this.reader.GetInt16(this.GetOrdinal(name));
            }
            catch (InvalidCastException ex)
            {
                throw new InvalidCastException("Invalid cast reading field '" + name + "', Reader: '" + this.Description + "'", ex);
            }
            catch (SqlTypes.SqlNullValueException ex)
            {
                throw new SqlTypes.SqlNullValueException("Field " + "'" + name + "' returned an invalid null.", ex);
            }
        }

        /// <summary>
        /// Gets the 32-bit signed integer value of the specified field.  Nulls are not allowed.
        /// </summary>
        /// <remarks>If the read returns a DBNull an exception is cast.</remarks>
        /// <param name="name">The name of the field to read.</param>
        /// <returns>The 32-bit signed integer value of the specified field.</returns>
        public int GetInt32NotNull(string name)
        {
            try
            {
                return this.reader.GetInt32(this.GetOrdinal(name));
            }
            catch (InvalidCastException ex)
            {
                throw new InvalidCastException("Invalid cast reading field '" + name + "', Reader: '" + this.Description + "'", ex);
            }
            catch (SqlTypes.SqlNullValueException ex)
            {
                throw new SqlTypes.SqlNullValueException("Field " + "'" + name + "' returned an invalid null.", ex);
            }
        }

        /// <summary>
        /// Gets the 64-bit signed integer value of the specified field.  Nulls are not allowed.
        /// </summary>
        /// <remarks>If the read returns a DBNull an exception is cast.</remarks>
        /// <param name="name">The name of the field to read.</param>
        /// <returns>The 64-bit signed integer value of the specified field</returns>
        public long GetInt64NotNull(string name)
        {
            try
            {
                return this.reader.GetInt64(this.GetOrdinal(name));
            }
            catch (InvalidCastException ex)
            {
                throw new InvalidCastException("Invalid cast reading field '" + name + "', Reader: '" + this.Description + "'", ex);
            }
            catch (SqlTypes.SqlNullValueException ex)
            {
                throw new SqlTypes.SqlNullValueException("Field " + "'" + name + "' returned an invalid null.", ex);
            }
        }

        /// <summary>
        /// Gets the string value of the specified field.  Nulls are not allowed.
        /// </summary>
        /// <remarks>If the read returns a DBNull an exception is cast.</remarks>
        /// <param name="name">The name of the field to read.</param>
        /// <returns>The string value of the specified field.</returns>
        public string GetStringNotNull(string name)
        {
            try
            {
                return this.reader.GetString(this.GetOrdinal(name));
            }
            catch (InvalidCastException ex)
            {
                throw new InvalidCastException("Invalid cast reading field '" + name + "', Reader: '" + this.Description + "'", ex);
            }
            catch (SqlTypes.SqlNullValueException ex)
            {
                throw new SqlTypes.SqlNullValueException("Field " + "'" + name + "' returned an invalid null, Reader: '" + this.Description + "'", ex);
            }
        }
        #endregion

        #region GetNullableX(string)
        /// <summary>
        /// Reads a boolean value from the specified field name.
        /// </summary>
        /// <param name="name">The name of the field to read.</param>
        /// <returns>The nullable value of the field.</returns>
        public Nullable<bool> GetNullableBoolean(string name)
        {
            try
            {
                return GetNullable<bool>(this.GetOrdinal(name), this.reader.GetBoolean);
            }
            catch (InvalidCastException ex)
            {
                throw new InvalidCastException("Invalid cast reading field '" + name + "', Reader: '" + this.Description + "'", ex);
            }
        }

        /// <summary>
        /// Gets the 8-bit unsigned integer value of the specified column.
        /// </summary>
        /// <param name="name">The name of the field to read.</param>
        /// <returns>The nullable 8-bit unsigned integer value of the specified column.</returns>
        public Nullable<byte> GetNullableByte(string name)
        {
            try
            {
                return GetNullable<byte>(this.GetOrdinal(name), this.reader.GetByte);
            }
            catch (InvalidCastException ex)
            {
                throw new InvalidCastException("Invalid cast reading field '" + name + "', Reader: '" + this.Description + "'", ex);
            }
        }

        /// <summary>
        /// Gets the character value of the specified column.
        /// </summary>
        /// <param name="name">The name of the field to read.</param>
        /// <returns>The nullable character value of the specified column.</returns>
        public Nullable<char> GetNullableChar(string name)
        {
            Nullable<char> nullable;
            int ordinal = this.GetOrdinal(name);

            if (this.reader.IsDBNull(ordinal))
            {
                nullable = null;
            }
            else
            {
                try
                {
                    nullable = this.GetChar(name);
                }
                catch (InvalidCastException ex)
                {
                    throw new InvalidCastException("Invalid cast reading field '" + name + "', Reader: '" + this.Description + "'", ex);
                }
            }

            return nullable;
        }

        /// <summary>
        /// Gets the date and time data value of the specified field.
        /// </summary>
        /// <param name="name">The name of the field to read.</param>
        /// <returns>The nullable date and time data value of the specified field.</returns>
        public Nullable<DateTime> GetNullableDateTime(string name)
        {
            try
            {
                return this.GetNullable<DateTime>(this.GetOrdinal(name), this.reader.GetDateTime);
            }
            catch (InvalidCastException ex)
            {
                throw new InvalidCastException("Invalid cast reading field '" + name + "', Reader: '" + this.Description + "'", ex);
            }
        }

        /// <summary>
        /// Gets the fixed-position numeric value of the specified field.
        /// </summary>
        /// <param name="name">The name of the field to read.</param>
        /// <returns>The nullable fixed-position numeric value of the specified field.</returns>
        public Nullable<decimal> GetNullableDecimal(string name)
        {
            try
            {
                return this.GetNullable<decimal>(this.GetOrdinal(name), this.reader.GetDecimal);
            }
            catch (InvalidCastException ex)
            {
                throw new InvalidCastException("Invalid cast reading field '" + name + "', Reader: '" + this.Description + "'", ex);
            }
        }

        /// <summary>
        /// Gets the double-precision floating point number of the specified field.
        /// </summary>
        /// <param name="name">The name of the field to read.</param>
        /// <returns>The nullable double-precision floating point number of the specified field.</returns>
        public Nullable<double> GetNullableDouble(string name)
        {
            try
            {
                return this.GetNullable<double>(this.GetOrdinal(name), this.reader.GetDouble);
            }
            catch (InvalidCastException ex)
            {
                throw new InvalidCastException("Invalid cast reading field '" + name + "', Reader: '" + this.Description + "'", ex);
            }
        }

        /// <summary>
        /// Gets the single-precision floating point number of the specified field.
        /// </summary>
        /// <param name="name">The name of the field to read.</param>
        /// <returns>The nullable single-precision floating point number of the specified field.</returns>
        public Nullable<float> GetNullableFloat(string name)
        {
            try
            {
                return this.GetNullable<float>(this.GetOrdinal(name), this.reader.GetFloat);
            }
            catch (InvalidCastException ex)
            {
                throw new InvalidCastException("Invalid cast reading field '" + name + "', Reader: '" + this.Description + "'", ex);
            }
        }

        /// <summary>
        /// Returns the GUID value of the specified field.
        /// </summary>
        /// <param name="name">The name of the field to read.</param>
        /// <returns>The nullable GUID value of the specified field.</returns>
        public Nullable<Guid> GetNullableGuid(string name)
        {
            try
            {
                return this.GetNullable<Guid>(this.GetOrdinal(name), this.reader.GetGuid);
            }
            catch (InvalidCastException ex)
            {
                throw new InvalidCastException("Invalid cast reading field '" + name + "', Reader: '" + this.Description + "'", ex);
            }
        }

        /// <summary>
        /// Gets the 16-bit signed integer value of the specified field.
        /// </summary>
        /// <param name="name">The name of the field to read.</param>
        /// <returns>The nullable 16-bit signed integer value of the specified field.</returns>
        public Nullable<short> GetNullableInt16(string name)
        {
            try
            {
                return this.GetNullable<short>(this.GetOrdinal(name), this.reader.GetInt16);
            }
            catch (InvalidCastException ex)
            {
                throw new InvalidCastException("Invalid cast reading field '" + name + "', Reader: '" + this.Description + "'", ex);
            }
        }

        /// <summary>
        /// Gets the 32-bit signed integer value of the specified field.
        /// </summary>
        /// <param name="name">The name of the field to read.</param>
        /// <returns>The nullable 32-bit signed integer value of the specified field.</returns>
        public Nullable<int> GetNullableInt32(string name)
        {
            try
            {
                return this.GetNullable<int>(this.GetOrdinal(name), this.reader.GetInt32);
            }
            catch (InvalidCastException ex)
            {
                throw new InvalidCastException("Invalid cast reading field '" + name + "', Reader: '" + this.Description + "'", ex);
            }
        }

        /// <summary>
        /// Gets the 64-bit signed integer value of the specified field.
        /// </summary>
        /// <param name="name">The name of the field to read.</param>
        /// <returns>The nullable 64-bit signed integer value of the specified field.</returns>
        public Nullable<long> GetNullableInt64(string name)
        {
            try
            {
                return this.GetNullable<long>(this.GetOrdinal(name), this.reader.GetInt64);
            }
            catch (InvalidCastException ex)
            {
                throw new InvalidCastException("Invalid cast reading field '" + name + "', Reader: '" + this.Description + "'", ex);
            }
        }
        #endregion
        #endregion //Business Methods

        #region Private methods
        /// <summary>
        /// Generic method to get a nullable type from a data reader
        /// </summary>
        /// <typeparam name="T">The type being read</typeparam>
        /// <param name="ordinal">Position of column in stream</param>
        /// <param name="convert">Delegate to do the reading</param>
        /// <returns>A nullable type of the field asked for</returns>
        private Nullable<T> GetNullable<T>(int ordinal, ConversionInt<T> convert) where T : struct
        {
            Nullable<T> nullable;

            if (this.reader.IsDBNull(ordinal))
            {
                nullable = null;
            }
            else
            {
                nullable = convert(ordinal);
            }

            return nullable;
        }

        /// <summary>
        /// Creates the internal collection of fieldnames used for looking up the number of columns by name.
        /// </summary>
        private void MakeOrdinalsTable()
        {
            this.keys.Clear();
            this.FieldNames = new string[this.reader.FieldCount];

            for (int i = 0; i < this.reader.FieldCount; i++)
            {
                string name = this.reader.GetName(i);

                if (this.keys.ContainsKey(name) == false)
                {
                    this.keys.Add(name, i);
                }

                this.FieldNames[i] = name;
            }
        }
        #endregion

        public void Dispose()
        {
            if (!IsClosed)
            {
                Close();
            }
        }
    }
}