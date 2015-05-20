package me.qinl.benchmark.avro;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Array;

/**
 * Created by qinlu on 15/5/18.
 */
public class AvroSpeed {

  private static final DecoderFactory DECODER_FACTORY = DecoderFactory.get();
  private static final EncoderFactory ENCODER_FACTORY = EncoderFactory.get();

  public static class GenericSerializer {

    private final GenericDatumWriter<GenericRecord> WRITER;
    private final GenericDatumReader<GenericRecord> READER;

    private BinaryEncoder encoder;
    private BinaryDecoder decoder;

    public GenericSerializer(Schema schema) {
      WRITER = new GenericDatumWriter<>(schema);
      READER = new GenericDatumReader<>(schema);
    }

    public GenericRecord deserialize(byte[] array) throws Exception {
      decoder = DECODER_FACTORY.binaryDecoder(array, decoder);
      return READER.read(null, decoder);
    }

    public byte[] serialize(GenericRecord data) throws IOException {
      ByteArrayOutputStream out = new ByteArrayOutputStream(1024);
      encoder = ENCODER_FACTORY.binaryEncoder(out, encoder);
      WRITER.write(data, encoder);
      encoder.flush();
      return out.toByteArray();
    }
  }

  public static final class SpecificSerializer<T> {

    private final SpecificDatumReader<T> READER;
    private final SpecificDatumWriter<T> WRITER;

    private BinaryEncoder encoder;
    private BinaryDecoder decoder;

    private final Class<T> clazz;

    public SpecificSerializer(Class<T> clazz) {
      this.clazz = clazz;
      this.READER = new SpecificDatumReader<T>(clazz);
      this.WRITER = new SpecificDatumWriter<T>(clazz);
    }

    public byte[] serialize(T content) throws Exception {
      ByteArrayOutputStream out = new ByteArrayOutputStream(1024);
      encoder = ENCODER_FACTORY.binaryEncoder(out, encoder);
      WRITER.write(content, encoder);
      encoder.flush();
      return out.toByteArray();
    }

    public void serializeItems(T[] items, OutputStream out) throws IOException {
      encoder = ENCODER_FACTORY.binaryEncoder(out, encoder);
      for (T item : items) {
        WRITER.write(item, encoder);
      }
      encoder.flush();
    }

    public T deserialize(byte[] array) throws Exception {
      decoder = DECODER_FACTORY.binaryDecoder(array, decoder);
      return READER.read(null, decoder);
    }

    public T[] deserializeItems(InputStream in0, int numberOfItems) throws IOException {
      decoder = DECODER_FACTORY.binaryDecoder(in0, decoder);
      @SuppressWarnings("unchecked")
      T[] result = (T[]) Array.newInstance(clazz, numberOfItems);
      T item = null;
      for (int i = 0; i < numberOfItems; ++i) {
        result[i] = READER.read(item, decoder);
      }
      return result;
    }
  }

}
