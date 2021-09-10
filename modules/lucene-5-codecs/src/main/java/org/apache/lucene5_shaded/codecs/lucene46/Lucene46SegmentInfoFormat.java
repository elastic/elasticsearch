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
package org.apache.lucene5_shaded.codecs.lucene46;

import java.io.IOException;
import java.text.ParseException;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import org.apache.lucene5_shaded.codecs.CodecUtil;
import org.apache.lucene5_shaded.codecs.SegmentInfoFormat;
import org.apache.lucene5_shaded.index.CorruptIndexException;
import org.apache.lucene5_shaded.index.IndexFileNames;
import org.apache.lucene5_shaded.index.SegmentInfo;
import org.apache.lucene5_shaded.store.ChecksumIndexInput;
import org.apache.lucene5_shaded.store.Directory;
import org.apache.lucene5_shaded.store.IOContext;
import org.apache.lucene5_shaded.util.Version;

/**
 * Lucene 4.6 Segment info format.
 * @deprecated only for old 4.x segments
 */
@Deprecated
public class Lucene46SegmentInfoFormat extends SegmentInfoFormat {

  /** Sole constructor. */
  public Lucene46SegmentInfoFormat() {
  }
  
  @Override
  public SegmentInfo read(Directory dir, String segment, byte segmentID[], IOContext context) throws IOException {
    final String fileName = IndexFileNames.segmentFileName(segment, "", Lucene46SegmentInfoFormat.SI_EXTENSION);
    try (ChecksumIndexInput input = dir.openChecksumInput(fileName, context)) {
      int codecVersion = CodecUtil.checkHeader(input, Lucene46SegmentInfoFormat.CODEC_NAME,
                                                      Lucene46SegmentInfoFormat.VERSION_START,
                                                      Lucene46SegmentInfoFormat.VERSION_CURRENT);
      final Version version;
      try {
        version = Version.parse(input.readString());
      } catch (ParseException pe) {
        throw new CorruptIndexException("unable to parse version string: " + pe.getMessage(), input, pe);
      }

      final int docCount = input.readInt();
      if (docCount < 0) {
        throw new CorruptIndexException("invalid docCount: " + docCount, input);
      }
      final boolean isCompoundFile = input.readByte() == SegmentInfo.YES;
      final Map<String,String> diagnostics = Collections.unmodifiableMap(input.readStringStringMap());
      final Set<String> files = Collections.unmodifiableSet(input.readStringSet());

      if (codecVersion >= Lucene46SegmentInfoFormat.VERSION_CHECKSUM) {
        CodecUtil.checkFooter(input);
      } else {
        CodecUtil.checkEOF(input);
      }

      final SegmentInfo si = new SegmentInfo(dir, version, segment, docCount, isCompoundFile, null, diagnostics, null, Collections.<String,String>emptyMap());
      si.setFiles(files);

      return si;
    }
  }

  @Override
  public void write(Directory dir, SegmentInfo info, IOContext ioContext) throws IOException {
    throw new UnsupportedOperationException("this codec can only be used for reading");
  }

  /** File extension used to store {@link SegmentInfo}. */
  final static String SI_EXTENSION = "si";
  static final String CODEC_NAME = "Lucene46SegmentInfo";
  static final int VERSION_START = 0;
  static final int VERSION_CHECKSUM = 1;
  static final int VERSION_CURRENT = VERSION_CHECKSUM;
}
