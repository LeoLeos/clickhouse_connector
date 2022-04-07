# ClickhouseConnector

clickhouse数据库读存操作

## 示例

实例化

```
con = ClickhouseConnector(server_name="test", dev=dev)
```

查询sql

```
con.query(sql)
```

保存df

```
con.save(sql)
```

显示数据库

显示数据库表名