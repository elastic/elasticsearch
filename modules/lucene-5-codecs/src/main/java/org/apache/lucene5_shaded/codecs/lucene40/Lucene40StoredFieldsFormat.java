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

import org.apache.lucene5_shaded.codecs.StoredFieldsFormat;
import org.apache.lucene5_shaded.codecs.StoredFieldsReader;
import org.apache.lucene5_shaded.codecs.StoredFieldsWriter;
import org.apache.lucene5_shaded.index.FieldInfos;
import org.apache.lucene5_shaded.index.SegmentInfo;
import org.apache.lucene5_shaded.store.Directory;
import org.apache.lucene5_shaded.store.IOContext;

/** 
 * Lucene 4.0 Stored Fields Format.
 * @deprecated only for reading 4.0 segments */
@Deprecated
public class Lucene40StoredFieldsFormat extends StoredFieldsFormat {

  /** Sole constructor. */
  public Lucene40StoredFieldsFormat() {
  }

  @Override
  public final StoredFieldsReader fieldsReader(Directory directory, SegmentInfo si,
      FieldInfos fn, IOContext context) throws IOException {
    return new Lucene40StoredFieldsReader(directory, si, fn, context);
  }

  @Override
  public StoredFieldsWriter fieldsWriter(Directory directory, SegmentInfo si,
      IOContext context) throws IOException {
    throw new UnsupportedOperationException("this codec can only be used for reading");
  }
}
