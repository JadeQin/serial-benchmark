package me.qinl.benchmark;

import cn.antvision.eagleattack.model.TestAvro;
import me.qinl.benchmark.avro.AvroSpeed;
import me.qinl.benchmark.json.JsonSpeed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by qinlu on 15/5/19.
 */
public class Main {

  private static final Logger LOG = LoggerFactory.getLogger(Main.class);

  public static void main(String[] args) throws Exception {

    JsonSpeed.TestJsonAvro jsonAvro = new JsonSpeed.TestJsonAvro();
    jsonAvro.id = "jsonAvro";

    TestAvro avro = new TestAvro("avro");

    byte[] avroBytes = new AvroSpeed.SpecificSerializer<>(TestAvro.class).serialize(avro);

    int count = 10000;

    long t1 = System.currentTimeMillis();
    for (int i = 0; i < count; i++) {
      new AvroSpeed.GenericSerializer(TestAvro.getClassSchema()).serialize(avro);
    }
    LOG.info("avro generic serial:" + (System.currentTimeMillis() - t1) + "");

    t1 = System.currentTimeMillis();
    for (int i = 0; i < count; i++) {
      new AvroSpeed.SpecificSerializer<>(TestAvro.class).serialize(avro);
    }
    LOG.info("avro specific serial:" + (System.currentTimeMillis() - t1) + "");

    t1 = System.currentTimeMillis();
    for (int i = 0; i < count; i++) {
      new JsonSpeed.JsonSimple<JsonSpeed.TestJsonAvro>().serial(jsonAvro);
    }
    LOG.info("json simple serial:" + (System.currentTimeMillis() - t1) + "");

    t1 = System.currentTimeMillis();
    for (int i = 0; i < count; i++) {
      new JsonSpeed.JsonAvro<>(TestAvro.getClassSchema()).json2Avro(jsonAvro);
    }
    LOG.info("json to avro serial:" + (System.currentTimeMillis() - t1) + "");

    t1 = System.currentTimeMillis();
    for (int i = 0; i < count; i++) {
      new JsonSpeed.JsonAvro<JsonSpeed.TestJsonAvro>(TestAvro.getClassSchema()).avro2Json(avroBytes, JsonSpeed.TestJsonAvro.class);
    }
    LOG.info("avro to json serial:" + (System.currentTimeMillis() - t1) + "");

  }
}
