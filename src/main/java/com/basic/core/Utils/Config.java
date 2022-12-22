package com.basic.core.Utils;

import com.basic.core.Topology;
import com.basic.core.TopologyArgs;
import com.google.common.collect.ImmutableList;
import java.io.FileInputStream;
import java.util.List;
import java.util.Properties;

public class Config {
  public static final List<String> SCHEMA = ImmutableList.of("relation", "timestamp", "seq" ,"key", "key2", "value");

  public static final String KAFKA_SPOUT_ID_R ="kafka-spout-r";
  public static final String KAFKA_SPOUT_ID_S ="kafka-spout-s";
  public static final String KAFKA_SPOUT_ID_T ="kafka-spout-t";
  public static final String SHUFFLE_BOLT_ID = "shuffle";
  public static final String DISPATCHER_BOLT_ID = "dispatcher";
  public static final String JOINER_R_BOLT_ID = "joiner-r";
  public static final String JOINER_S_BOLT_ID = "joiner-s";
  public static final String JOINER_RS_BOLT_ID = "joiner-rs";
  public static final String JOINER_T_BOLT_ID = "joiner-t";
  public static final String DUPLICATE_BOLT_ID = "duplicate";
  public static final String COLLECTOR_BOLT_ID = "collector";
  public static final String METRIC_BOLT_ID = "post-process";

  public static final String SHUFFLE_R_STREAM_ID = "shuffle-r";
  public static final String SHUFFLE_S_STREAM_ID = "shuffle-s";
  public static final String SHUFFLE_T_STREAM_ID = "shuffle-t";
  public static final String BROADCAST_R_STREAM_ID = "broadcast-r";
  public static final String BROADCAST_S_STREAM_ID = "broadcast-s";
  public static final String BROADCAST_T_STREAM_ID = "broadcast-t";
  public static final String JOIN_RESULTS_STREAM_ID = "join-results";
  public static final String METRIC_STREAM_ID = "metric-stream";
  public static final String DUPLICATE_STREAM_ID = "duplicate";
  public static final String RS_RESULTSTREAM_ID = "RS-result";
  public static final String SR_RESULTSTREAM_ID = "sR-result";
  public static final String ST_RESULTSTREAM_ID = "sT-result";
  public static final String TIMESTAMP_SEQ_ID = "timestamp-sequence";
  public static final String UV_RESULTSTREAM_ID = "uv-reslut";
//  public static final String RST_RESULTSTREAM_ID = "rst-reslut";

  public static final String KAFKA_SPOUT_ID_U ="kafka-spout-u";
  public static final String KAFKA_SPOUT_ID_V ="kafka-spout-v";

  public static final String SHUFFLE_U_STREAM_ID = "shuffle-u";
  public static final String SHUFFLE_V_STREAM_ID = "shuffle-v";
  public static final String SHUFFLE_UV_STREAM_ID = "shuffle-uv";
  public static final String SHUFFLE_RST_STREAM_ID = "shuffle-rst";
  public static final String BROADCAST_U_STREAM_ID = "broadcast-u";
  public static final String BROADCAST_V_STREAM_ID = "broadcast-v";
  public static final String BROADCAST_UV_STREAM_ID = "broadcast-uv";
  public static final String BROADCAST_RST_STREAM_ID = "broadcast-rst";
  public static final String JOINER_RST_BOLT_ID = "joiner-rst";
  public static final String JOINER_U_BOLT_ID = "joiner-u";
  public static final String JOINER_V_BOLT_ID = "joiner-v";
  public static final String JOINER_UV_BOLT_ID = "joiner-uv";
}
