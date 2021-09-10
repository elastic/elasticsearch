/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.lucene5_shaded.codecs.perfield;


import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.TreeMap;

import org.apache.lucene5_shaded.codecs.DocValuesConsumer;
import org.apache.lucene5_shaded.codecs.DocValuesFormat;
import org.apache.lucene5_shaded.codecs.DocValuesProducer;
import org.apache.lucene5_shaded.codecs.PostingsFormat;
import org.apache.lucene5_shaded.index.BinaryDocValues;
import org.apache.lucene5_shaded.index.DocValuesType;
import org.apache.lucene5_shaded.index.FieldInfo;
import org.apache.lucene5_shaded.index.NumericDocValues;
import org.apache.lucene5_shaded.index.SegmentReadState;
import org.apache.lucene5_shaded.index.SegmentWriteState;
import org.apache.lucene5_shaded.index.SortedDocValues;
import org.apache.lucene5_shaded.index.SortedNumericDocValues;
import org.apache.lucene5_shaded.index.SortedSetDocValues;
import org.apache.lucene5_shaded.util.Accountable;
import org.apache.lucene5_shaded.util.Accountables;
import org.apache.lucene5_shaded.util.Bits;
import org.apache.lucene5_shaded.util.BytesRef;
import org.apache.lucene5_shaded.util.IOUtils;
import org.apache.lucene5_shaded.util.RamUsageEstimator;

/**
 * Enables per field docvalues support.
 * <p>
 * Note, when extending this class, the name ({@link #getName}) is 
 * written into the index. In order for the field to be read, the
 * name must resolve to your implementation via {@link #forName(String)}.
 * This method uses Java's 
 * {@link ServiceLoader Service Provider Interface} to resolve format names.
 * <p>
 * Files written by each docvalues format have an additional suffix containing the 
 * format name. For example, in a per-field configuration instead of <tt>_1.dat</tt> 
 * filenames would look like <tt>_1_Lucene40_0.dat</tt>.
 * @see ServiceLoader
 * @lucene.experimental
 */

public abstract class PerFieldDocValuesFormat extends DocValuesFormat {
  /** Name of this {@link PostingsFormat}. */
  public static final String PER_FIELD_NAME = "PerFieldDV40";

  /** {@link FieldInfo} attribute name used to store the
   *  format name for each field. */
  public static final String PER_FIELD_FORMAT_KEY = PerFieldDocValuesFormat.class.getSimpleName() + ".format";

  /** {@link FieldInfo} attribute name used to store the
   *  segment suffix name for each field. */
  public static final String PER_FIELD_SUFFIX_KEY = PerFieldDocValuesFormat.class.getSimpleName() + ".suffix";

  
  /** Sole constructor. */
  public PerFieldDocValuesFormat() {
    super(PER_FIELD_NAME);
  }

  @Override
  public final DocValuesConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
    return new FieldsWriter(state);
  }

  static class ConsumerAndSuffix implements Closeable {
    DocValuesConsumer consumer;
    int suffix;
    
    @Override
    public void close() throws IOException {
      consumer.close();
    }
  }
    
  private class FieldsWriter extends DocValuesConsumer {

    private final Map<DocValuesFormat,ConsumerAndSuffix> formats = new HashMap<>();
    private final Map<String,Integer> suffixes = new HashMap<>();
    
    private final SegmentWriteState segmentWriteState;
    
    public FieldsWriter(SegmentWriteState state) {
      segmentWriteState = state;
    }
    
    @Override
    public void addNumericField(FieldInfo field, Iterable<Number> values) throws IOException {
      getInstance(field).addNumericField(field, values);
    }

    @Override
    public void addBinaryField(FieldInfo field, Iterable<BytesRef> values) throws IOException {
      getInstance(field).addBinaryField(field, values);
    }

    @Override
    public void addSortedField(FieldInfo field, Iterable<BytesRef> values, Iterable<Number> docToOrd) throws IOException {
      getInstance(field).addSortedField(field, values, docToOrd);
    }

    @Override
    public void addSortedNumericField(FieldInfo field, Iterable<Number> docToValueCount, Iterable<Number> values) throws IOException {
      getInstance(field).addSortedNumericField(field, docToValueCount, values);
    }

    @Override
    public void addSortedSetField(FieldInfo field, Iterable<BytesRef> values, Iterable<Number> docToOrdCount, Iterable<Number> ords) throws IOException {
      getInstance(field).addSortedSetField(field, values, docToOrdCount, ords);
    }

    private DocValuesConsumer getInstance(FieldInfo field) throws IOException {
      DocValuesFormat format = null;
      if (field.getDocValuesGen() != -1) {
        final String formatName = field.getAttribute(PER_FIELD_FORMAT_KEY);
        // this means the field never existed in that segment, yet is applied updates
        if (formatName != null) {
          format = DocValuesFormat.forName(formatName);
        }
      }
      if (format == null) {
        format = getDocValuesFormatForField(field.name);
      }
      if (format == null) {
        throw new IllegalStateException("invalid null DocValuesFormat for field=\"" + field.name + "\"");
      }
      final String formatName = format.getName();
      
      String previousValue = field.putAttribute(PER_FIELD_FORMAT_KEY, formatName);
      if (field.getDocValuesGen() == -1 && previousValue != null) {
        throw new IllegalStateException("found existing value for " + PER_FIELD_FORMAT_KEY + 
                                        ", field=" + field.name + ", old=" + previousValue + ", new=" + formatName);
      }
      
      Integer suffix = null;
      
      ConsumerAndSuffix consumer = formats.get(format);
      if (consumer == null) {
        // First time we are seeing this format; create a new instance

        if (field.getDocValuesGen() != -1) {
          final String suffixAtt = field.getAttribute(PER_FIELD_SUFFIX_KEY);
          // even when dvGen is != -1, it can still be a new field, that never
          // existed in the segment, and therefore doesn't have the recorded
          // attributes yet.
          if (suffixAtt != null) {
            suffix = Integer.valueOf(suffixAtt);
          }
        }
        
        if (suffix == null) {
          // bump the suffix
          suffix = suffixes.get(formatName);
          if (suffix == null) {
            suffix = 0;
          } else {
            suffix = suffix + 1;
          }
        }
        suffixes.put(formatName, suffix);
        
        final String segmentSuffix = getFullSegmentSuffix(segmentWriteState.segmentSuffix,
                                                          getSuffix(formatName, Integer.toString(suffix)));
        consumer = new ConsumerAndSuffix();
        consumer.consumer = format.fieldsConsumer(new SegmentWriteState(segmentWriteState, segmentSuffix));
        consumer.suffix = suffix;
        formats.put(format, consumer);
      } else {
        // we've already seen this format, so just grab its suffix
        assert suffixes.containsKey(formatName);
        suffix = consumer.suffix;
      }
      
      previousValue = field.putAttribute(PER_FIELD_SUFFIX_KEY, Integer.toString(suffix));
      if (field.getDocValuesGen() == -1 && previousValue != null) {
        throw new IllegalStateException("found existing value for " + PER_FIELD_SUFFIX_KEY + 
                                        ", field=" + field.name + ", old=" + previousValue + ", new=" + suffix);
      }

      // TODO: we should only provide the "slice" of FIS
      // that this DVF actually sees ...
      return consumer.consumer;
    }

    @Override
    public void close() throws IOException {
      // Close all subs
      IOUtils.close(formats.values());
    }
  }
  
  static String getSuffix(String formatName, String suffix) {
    return formatName + "_" + suffix;
  }

  static String getFullSegmentSuffix(String outerSegmentSuffix, String segmentSuffix) {
    if (outerSegmentSuffix.length() == 0) {
      return segmentSuffix;
    } else {
      return outerSegmentSuffix + "_" + segmentSuffix;
    }
  }

  private class FieldsReader extends DocValuesProducer {

    private final Map<String,DocValuesProducer> fields = new TreeMap<>();
    private final Map<String,DocValuesProducer> formats = new HashMap<>();
    
    // clone for merge
    FieldsReader(FieldsReader other) throws IOException {
      Map<DocValuesProducer,DocValuesProducer> oldToNew = new IdentityHashMap<>();
      // First clone all formats
      for(Map.Entry<String,DocValuesProducer> ent : other.formats.entrySet()) {
        DocValuesProducer values = ent.getValue().getMergeInstance();
        formats.put(ent.getKey(), values);
        oldToNew.put(ent.getValue(), values);
      }

      // Then rebuild fields:
      for(Map.Entry<String,DocValuesProducer> ent : other.fields.entrySet()) {
        DocValuesProducer producer = oldToNew.get(ent.getValue());
        assert producer != null;
        fields.put(ent.getKey(), producer);
      }
    }

    public FieldsReader(final SegmentReadState readState) throws IOException {

      // Init each unique format:
      boolean success = false;
      try {
        // Read field name -> format name
        for (FieldInfo fi : readState.fieldInfos) {
          if (fi.getDocValuesType() != DocValuesType.NONE) {
            final String fieldName = fi.name;
            final String formatName = fi.getAttribute(PER_FIELD_FORMAT_KEY);
            if (formatName != null) {
              // null formatName means the field is in fieldInfos, but has no docvalues!
              final String suffix = fi.getAttribute(PER_FIELD_SUFFIX_KEY);
              if (suffix == null) {
                throw new IllegalStateException("missing attribute: " + PER_FIELD_SUFFIX_KEY + " for field: " + fieldName);
              }
              DocValuesFormat format = DocValuesFormat.forName(formatName);
              String segmentSuffix = getFullSegmentSuffix(readState.segmentSuffix, getSuffix(formatName, suffix));
              if (!formats.containsKey(segmentSuffix)) {
                formats.put(segmentSuffix, format.fieldsProducer(new SegmentReadState(readState, segmentSuffix)));
              }
              fields.put(fieldName, formats.get(segmentSuffix));
            }
          }
        }
        success = true;
      } finally {
        if (!success) {
          IOUtils.closeWhileHandlingException(formats.values());
        }
      }
    }

    @Override
    public NumericDocValues getNumeric(FieldInfo field) throws IOException {
      DocValuesProducer producer = fields.get(field.name);
      return producer == null ? null : producer.getNumeric(field);
    }

    @Override
    public BinaryDocValues getBinary(FieldInfo field) throws IOException {
      DocValuesProducer producer = fields.get(field.name);
      return producer == null ? null : producer.getBinary(field);
    }

    @Override
    public SortedDocValues getSorted(FieldInfo field) throws IOException {
      DocValuesProducer producer = fields.get(field.name);
      return producer == null ? null : producer.getSorted(field);
    }

    @Override
    public SortedNumericDocValues getSortedNumeric(FieldInfo field) throws IOException {
      DocValuesProducer producer = fields.get(field.name);
      return producer == null ? null : producer.getSortedNumeric(field);
    }

    @Override
    public SortedSetDocValues getSortedSet(FieldInfo field) throws IOException {
      DocValuesProducer producer = fields.get(field.name);
      return producer == null ? null : producer.getSortedSet(field);
    }
    
    @Override
    public Bits getDocsWithField(FieldInfo field) throws IOException {
      DocValuesProducer producer = fields.get(field.name);
      return producer == null ? null : producer.getDocsWithField(field);
    }

    @Override
    public void close() throws IOException {
      IOUtils.close(formats.values());
    }

    @Override
    public long ramBytesUsed() {
      long size = 0;
      for (Map.Entry<String,DocValuesProducer> entry : formats.entrySet()) {
        size += (entry.getKey().length() * RamUsageEstimator.NUM_BYTES_CHAR) + 
            entry.getValue().ramBytesUsed();
      }
      return size;
    }
    
    @Override
    public Collection<Accountable> getChildResources() {
      return Accountables.namedAccountables("format", formats);
    }

    @Override
    public void checkIntegrity() throws IOException {
      for (DocValuesProducer format : formats.values()) {
        format.checkIntegrity();
      }
    }
    
    @Override
    public DocValuesProducer getMergeInstance() throws IOException {
      return new FieldsReader(this);
    }

    @Override
    public String toString() {
      return "PerFieldDocValues(formats=" + formats.size() + ")";
    }
  }

  @Override
  public final DocValuesProducer fieldsProducer(SegmentReadState state) throws IOException {
    return new FieldsReader(state);
  }

  /** 
   * Returns the doc values format that should be used for writing 
   * new segments of <code>field</code>.
   * <p>
   * The field to format mapping is written to the index, so
   * this method is only invoked when writing, not when reading. */
  public abstract DocValuesFormat getDocValuesFormatForField(String field);
}
