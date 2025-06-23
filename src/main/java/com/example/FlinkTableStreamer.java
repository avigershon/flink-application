package com.example;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import java.util.List;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.api.common.functions.MapFunction;

public class FlinkTableStreamer {

    private static Map<String, String> parseArgs(String[] args) {
        Map<String, String> map = new HashMap<>();
        for (int i = 0; i < args.length; i++) {
            if (args[i].startsWith("--") && i + 1 < args.length) {
                String key = args[i].substring(2);
                map.put(key, args[++i]);
            }
        }
        return map;
    }

    private static String extractTableName(String ddl) {
        Pattern p = Pattern.compile("CREATE\\s+TABLE\\s+`?(\\w+)`?", Pattern.CASE_INSENSITIVE);
        Matcher m = p.matcher(ddl);
        if (m.find()) {
            return m.group(1);
        }
        throw new IllegalArgumentException("Could not extract table name from DDL");
    }

    private static String stripAsSelect(String ddl) {
        int idx = ddl.toUpperCase().indexOf(" AS SELECT");
        if (idx != -1) {
            return ddl.substring(0, idx);
        }
        return ddl;
    }

    public static void main(String[] args) throws Exception {
        Map<String, String> params = parseArgs(args);
        String sourceDDL = params.get("source_ddl");
        String sinkDDL = params.get("sink_ddl");
        String field = params.get("field");
        String value = params.get("value");

        if (sourceDDL == null || sinkDDL == null || field == null || value == null) {
            System.err.println("Required arguments: --source_ddl <ddl> --sink_ddl <ddl> --field <field> --value <value>");
            return;
        }
        sourceDDL = stripAsSelect(sourceDDL);
        sinkDDL = stripAsSelect(sinkDDL);

        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

        tEnv.executeSql(sourceDDL);
        String sourceTable = extractTableName(sourceDDL);

        tEnv.executeSql(sinkDDL);
        String sinkTable = extractTableName(sinkDDL);

        Table source = tEnv.from(sourceTable);
        DataStream<Row> stream = tEnv.toDataStream(source);

        List<String> names = source.getResolvedSchema().getColumnNames();
        final int index = names.indexOf(field);
        if (index < 0) {
            throw new IllegalArgumentException("Field not found: " + field);
        }

        DataStream<Row> modifiedStream = stream.map(new MapFunction<Row, Row>() {
            @Override
            public Row map(Row r) {
                Row copy = new Row(r.getArity());
                for (int i = 0; i < r.getArity(); i++) {
                    copy.setField(i, r.getField(i));
                }
                copy.setField(index, value);
                return copy;
            }
        });

        Table modified = tEnv.fromDataStream(modifiedStream).as(names.toArray(new String[0]));
        modified.executeInsert(sinkTable).await();
    }
}
