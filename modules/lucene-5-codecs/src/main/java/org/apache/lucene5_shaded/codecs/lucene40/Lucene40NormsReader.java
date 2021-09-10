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
package org.apache.lucene5_shaded.codecs.lucene40;

import java.io.IOException;
import java.util.Collection;

import org.apache.lucene5_shaded.codecs.DocValuesProducer;
import org.apache.lucene5_shaded.codecs.NormsProducer;
import org.apache.lucene5_shaded.codecs.UndeadNormsProducer;
import org.apache.lucene5_shaded.index.DocValues;
import org.apache.lucene5_shaded.index.FieldInfo;
import org.apache.lucene5_shaded.index.NumericDocValues;
import org.apache.lucene5_shaded.index.SegmentReadState;
import org.apache.lucene5_shaded.util.Accountable;

/**
 * Reads 4.0/4.1 norms.
 * @deprecated Only for reading old 4.0 and 4.1 segments
 */
@Deprecated
final class Lucene40NormsReader extends NormsProducer {
  private final DocValuesProducer impl;
  
  // clone for merge
  Lucene40NormsReader(DocValuesProducer impl) throws IOException {
    this.impl = impl.getMergeInstance();
  }
  
  Lucene40NormsReader(SegmentReadState state, String filename) throws IOException {
    impl = new Lucene40DocValuesReader(state, filename, Lucene40FieldInfosFormat.LEGACY_NORM_TYPE_KEY);
  }
  
  @Override
  public NumericDocValues getNorms(FieldInfo field) throws IOException {
    if (UndeadNormsProducer.isUndead(field)) {
      // Bring undead norms back to life; this is set in Lucene40FieldInfosFormat, to emulate pre-5.0 undead norms
      return DocValues.emptyNumeric();
    }
    return impl.getNumeric(field);
  }
  
  @Override
  public void close() throws IOException {
    impl.close();
  }

  @Override
  public long ramBytesUsed() {
    return impl.ramBytesUsed();
  }
  
  @Override
  public Collection<Accountable> getChildResources() {
    return impl.getChildResources();
  }

  @Override
  public void checkIntegrity() throws IOException {
    impl.checkIntegrity();
  }
  
  @Override
  public NormsProducer getMergeInstance() throws IOException {
    return new Lucene40NormsReader(impl);
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "(" + impl + ")";
  }
}
