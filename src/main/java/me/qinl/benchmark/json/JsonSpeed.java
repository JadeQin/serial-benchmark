package me.qinl.benchmark.json;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.avro.AvroFactory;
import com.fasterxml.jackson.dataformat.avro.AvroSchema;
import org.apache.avro.Schema;

import java.io.Serializable;

/**
 * Created by qinlu on 15/5/18.
 */
public class JsonSpeed {

  public static class JsonSimple<T> {

    public byte[] serial(T data) throws Exception {
      ObjectMapper mapper = new ObjectMapper();
      return mapper.writeValueAsBytes(data);
    }

    public T deserial(byte[] data, Class<T> clazz) throws Exception {
      ObjectMapper mapper = new ObjectMapper();
      return mapper.reader(clazz).readValue(data);
    }
  }

  public static class JsonAvro<T> {

    private Schema srcSchema;

    public JsonAvro(Schema schema) {
      this.srcSchema = schema;
    }

    public byte[] json2Avro(T data) throws Exception {
      ObjectMapper mapper = new ObjectMapper(new AvroFactory());
      AvroSchema schema = new AvroSchema(srcSchema);
      return mapper.writer(schema).writeValueAsBytes(data);
    }

    public T avro2Json(byte[] data, Class<T> clazz) throws Exception {
      ObjectMapper mapper = new ObjectMapper(new AvroFactory());
      AvroSchema schema = new AvroSchema(srcSchema);
      return mapper.reader(clazz).with(schema).readValue(data);
    }

  }

  public static class TestJsonAvro implements Serializable {
    public String id;
  }

}
