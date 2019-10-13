using System.Data;
using System.Data.SqlClient;

namespace KG.System.Data.SqlClient.Extensions.ReaderWrapper
{
    public class SqlParam
    {
        private string _name;
        private SqlDbType _dbType;
        private object _value;

        public SqlParam(string name, SqlDbType dbType, object value)
        {
            _name = name;
            _dbType = dbType;
            _value = value;
        }

        public SqlParameter Value => new SqlParameter(_name, _dbType) { Value = _value };
    }
}
