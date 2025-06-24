package bi.flink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.util.ParameterTool;

import static org.apache.flink.table.api.Expressions.*;

public class MainApp {

  // Example User-Defined Function
  public static class ProcessNameFunction extends ScalarFunction {
    public String eval(String name) {
      if (name == null) {
        return "UNKNOWN";
      }
      // Add your custom processing logic here
      return name.toUpperCase().trim() + "_PROCESSED";
    }
  }

  // Example User-Defined Function for ID processing
  public static class ProcessIdFunction extends ScalarFunction {
    public Long eval(Long id) {
      if (id == null) {
        return 0L;
      }
      // Add your custom processing logic here
      return id * 100; // Example: multiply by 100
    }
  }

  public static void main(String[] args) {

    try {

      //setup env
      Configuration envConf = new Configuration();
      final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(envConf);

      EnvironmentSettings settings = EnvironmentSettings
              .newInstance()
              .inStreamingMode()
              .build();

      StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

      ParameterTool params = ParameterTool.fromArgs(args);
      String ddlList = params.get("dbt.ddl");

      for (String stmt : ddlList.split("\n")) {
        if (!stmt.isBlank()) {
          tEnv.executeSql(stmt);
        }
      }

      // Register User-Defined Functions
      tEnv.createTemporarySystemFunction("PROCESS_NAME", ProcessNameFunction.class);
      tEnv.createTemporarySystemFunction("PROCESS_ID", ProcessIdFunction.class);

      Table sourceTable = tEnv.from("users");

      Table processedTable = sourceTable
              .select(
                      call("PROCESS_ID", $("id")).as("id"),
                      call("PROCESS_NAME", $("name")).as("name")
              )
              .filter($("id").isGreater(0L))  // Filter out invalid IDs
              .filter($("name").isNotNull()); // Filter out null names

      processedTable.executeInsert("casino_activities");

    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}