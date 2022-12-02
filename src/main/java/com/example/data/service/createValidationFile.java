package com.example.data.service;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.csvreader.CsvWriter;
import com.example.data.resultutil.Result;
import com.xugu.cloudjdbc.Connection;
import com.xugu.cloudjdbc.Statement;
import org.apache.commons.codec.digest.DigestUtils;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;


/**
 * @Auther zx
 * @Date: 2022/11/17/17:13
 */
public class createValidationFile {

    public Result createValidationFiles(Connection connection, String databaseName, String tableName, String areaCode, String indexColumn, String compareColumns, String queryField, List<Map<String, String>> queryTimes, String localFilePath) {
        String[] split = compareColumns.split(",");
        StringBuilder buffer = new StringBuilder();
        StringBuilder bufferResultVal = new StringBuilder();
        Map<String[], String[]> vMap = new HashMap<>();
        try {
            return createSql(connection, databaseName, tableName, areaCode, indexColumn, queryField, queryTimes, split, buffer, bufferResultVal, vMap, localFilePath);
        } catch (Exception e) {
            return Result.error500(e.getMessage(), "");
        }
    }

    private Result createSql(Connection connection, String databaseName, String tableName, String areaCode, String indexColumn, String queryField, List<Map<String, String>> queryTimes, String[] split, StringBuilder buffer, StringBuilder bufferResultVal, Map<String[], String[]> vMap, String localFilePath) throws SQLException, ParseException {
        String value;
        ArrayList<String> list = new ArrayList<>();
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        SimpleDateFormat formatTo = new SimpleDateFormat("yyyyMMddHHmmss");
        for (Map<String, String> queryTimesMap : queryTimes) {
            for (Map.Entry<String, String> m : queryTimesMap.entrySet()) {
                buffer.append("select ");
                for (int j = 0; j < split.length; j++) {
                    if (j == split.length - 1) {
                        buffer.append(split[j]);
                    } else {
                        buffer.append(split[j]).append(",");
                    }
                }
                buffer.append(" from ").append(tableName).append(" where ");
                buffer.append(queryField + " >= " + "'" + m.getKey() + "'" + " and " + queryField + " < " + "'" + m.getValue() + "'");
                Statement statement = (Statement) connection.createStatement();
                ResultSet resultSet = statement.executeQuery(String.valueOf(buffer));
                //通过结果集获取元数据对象
                ResultSetMetaData metaData = resultSet.getMetaData();
                int columnCount = metaData.getColumnCount();
                while (resultSet.next()) {
                    for (int j = 1; j <= columnCount; j++) {
                        String c = resultSet.getMetaData().getColumnName(j);
                        String typeName = resultSet.getMetaData().getColumnTypeName(j);
                        value = String.valueOf(resultSet.getObject(c));
                        if ("DATETIME".equals(typeName)) {
                            value = format.format(resultSet.getObject(c));
                        }
                        if ("".equals(value) || null == value) {
                            value = "";
                        }
                        bufferResultVal.append(value).append("&");
                    }
                }
                if (bufferResultVal.length() == 0) {
                    return Result.error500("表中数据为空", "");
                }
                JSON tableColumns = getTableColumns(tableName, connection);
                vMap.put(new String[]{indexColumn}, new String[]{String.valueOf(bufferResultVal.deleteCharAt(bufferResultVal.length() - 1))});
                //文件名日期转换
                Date begin = format.parse(m.getKey());
                Date end = format.parse(m.getValue());
                String beginTime = formatTo.format(begin);
                String endTime = formatTo.format(end);
                bufferResultVal.delete(0, bufferResultVal.length());
                buffer.delete(0, buffer.length());
                String s = writeCSV(databaseName, areaCode, tableName, localFilePath, vMap, beginTime, endTime, tableColumns);
                list.add(s);
                statement.close();
            }
        }
        return Result.ok("success", list);
    }

    //获取表列信息
    private JSON getTableColumns(String tableName, Connection connection) throws SQLException {
        JSONArray jsonArray = new JSONArray();
        ArrayList<String> list = new ArrayList<>();
        String[] resultArray;
        //获取主键和默认值
        String username = connection.getUser();
        String pkSql = "SELECT define FROM USER_CONSTRAINTS WHERE table_id IN (SELECT table_id FROM all_tables WHERE user_id " +
                "IN (SELECT user_id FROM  ALL_USERS WHERE USER_NAME='" + username + "' AND table_name ='" + tableName + "')) AND CONS_TYPE = 'p'";
        PreparedStatement statement = connection.prepareStatement("select * from " + tableName);
        String defValueSql = "Select def_val from ALL_COLUMNS ac INNER join  (SELECT table_id FROM all_tables atb inner JOIN  " +
                "ALL_USERS au ON atb.USER_id= au.user_id and au.USER_NAME='" + username + "' AND atb.table_name='" + tableName + "')" +
                "AS t on ac.table_id = t.table_id";
        PreparedStatement defStatement = connection.prepareStatement(defValueSql);
        PreparedStatement pkStatement = connection.prepareStatement(pkSql);
        ResultSet defResultSet = defStatement.executeQuery();
        ResultSet pkResultSet = pkStatement.executeQuery();
        ResultSetMetaData metaData = statement.getMetaData();
        int columnCount = metaData.getColumnCount();
        String valuePk = "";
        while (pkResultSet.next()) {
            for (int i = 1; i <= pkStatement.getMetaData().getColumnCount(); i++) {
                String c = pkResultSet.getMetaData().getColumnName(i);
                valuePk = pkResultSet.getObject(c) + ",";
            }
        }
        resultArray = valuePk.split(",");
        if (valuePk.length() < 1) {
            resultArray = null;
        }
        //遍历元数据对象属性
        for (int j = 1; j <= columnCount; j++) {
            JSONObject json = new JSONObject(true);
            //列名
            json.put("columnName", metaData.getColumnName(j));
            //列类型
            json.put("fileType", metaData.getColumnTypeName(j));
            //精度
            json.put("accuracy", metaData.getPrecision(j));
            //是否为空
            json.put("isNull", metaData.isNullable(j));
            //先把默认值统一为空
            json.put("defaultValue", "");
            //查询是否主键
            assert resultArray != null;
            for (String s : resultArray) {
                s = s.replace("\"", "");
                if (metaData.getColumnName(j).equalsIgnoreCase(s)) {
                    json.put("isPrimaryKey", "1");
                    break;
                }
            }
            json.putIfAbsent("isPrimaryKey", "0");
            while (defResultSet.next()) {
                for (int i = 1; i <= defStatement.getMetaData().getColumnCount(); i++) {
                    String c = defResultSet.getMetaData().getColumnName(j);
                    String value = String.valueOf(defResultSet.getObject(c));
                    list.add(value);
                }
            }
            //设置默认值
            json.put("defaultValue", list.get(j - 1));
            jsonArray.add(json);
        }
        pkStatement.close();
        defStatement.close();
        return jsonArray;
    }


    //将数据写入csv文件方法
    private String writeCSV(String databaseName, String areaCode, String tableName, String localFilePath, Map<String[], String[]> vMap, String beginTime, String endTime, JSON tableColumns) {
        String filename = localFilePath + "\\" + areaCode + "_" + databaseName + "_" + tableName + "_" + beginTime + "-" + endTime + ".csv";
        File file = new File(filename);
        CsvWriter csvWriter;
        try {
            if (!file.exists()) {
                file.createNewFile();
            }
            csvWriter = new CsvWriter(filename, ',', StandardCharsets.UTF_8);
            String[] headers = {"indexColumns", "md5"};
            csvWriter.writeRecord(headers);
            csvWriter.writeRecord(new String[]{String.valueOf(tableColumns)});
            for (Map.Entry<String[], String[]> map : vMap.entrySet()) {
                String[] content = {DigestUtils.md5Hex(map.getKey()[0]), DigestUtils.md5Hex(Arrays.toString(map.getValue())).toUpperCase()};
                csvWriter.writeRecord(content);
            }
        } catch (IOException e) {
            return String.valueOf(Result.error500(e.getMessage(), ""));
        }
        csvWriter.close();
        return filename;
    }


}
