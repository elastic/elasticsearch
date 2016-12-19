/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
public class LengthEncodedWriter implements RecordWriter {
    private OutputStream outputStream;
    private ByteBuffer lengthBuffer;

    /**
     * Create the writer on the OutputStream <code>os</code>.
     * This object will never close <code>os</code>.
     */
    public LengthEncodedWriter(OutputStream os) {
        outputStream = os;
        // This will be used to convert 32 bit integers to network byte order
        lengthBuffer = ByteBuffer.allocate(4); // 4 == sizeof(int)
    }


    /**
     * Convert each String in the record array to a length/value encoded pair
     * and write to the outputstream.
     */
    @Override
    public void writeRecord(String[] record) throws IOException {
        writeNumFields(record.length);

        for (String field : record) {
            writeField(field);
        }
    }

    /**
     * Convert each String in the record list to a length/value encoded
     * pair and write to the outputstream.
     */
    @Override
    public void writeRecord(List<String> record) throws IOException {
        writeNumFields(record.size());

        for (String field : record) {
            writeField(field);
        }
    }


    /**
     * Lower level functions to write records individually.
     * After this function is called {@link #writeField(String)}
     * must be called <code>numFields</code> times.
     */
    public void writeNumFields(int numFields) throws IOException {
        // number fields
        lengthBuffer.clear();
        lengthBuffer.putInt(numFields);
        outputStream.write(lengthBuffer.array());
    }


    /**
     * Lower level functions to write record fields individually.
     * {@linkplain #writeNumFields(int)} must be called first
     */
    public void writeField(String field) throws IOException {
        byte[] utf8Bytes = field.getBytes(StandardCharsets.UTF_8);
        lengthBuffer.clear();
        lengthBuffer.putInt(utf8Bytes.length);
        outputStream.write(lengthBuffer.array());
        outputStream.write(utf8Bytes);
    }

    @Override
    public void flush() throws IOException {
        outputStream.flush();
    }
}
