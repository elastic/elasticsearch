package org.apache.lucene.search.suggest.xdocument;

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

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.FieldsConsumer;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.util.fst.FST;

import java.io.IOException;

/**
 * <p>
 * A {@link PostingsFormat} which supports document suggestion based on
 * indexed {@link SuggestField}s.
 * Document suggestion is based on an weighted FST which map analyzed
 * terms of a {@link SuggestField} to its surface form and document id.
 * </p>
 * <p>
 * Files:
 * <ul>
 *   <li><tt>.lkp</tt>: <a href="#Completiondictionary">Completion Dictionary</a></li>
 *   <li><tt>.cmp</tt>: <a href="#Completionindex">Completion Index</a></li>
 * </ul>
 * <p>
 * <a name="Completionictionary"></a>
 * <h3>Completion Dictionary</h3>
 * <p>The .lkp file contains an FST for each suggest field
 * </p>
 * <ul>
 *   <li>CompletionDict (.lkp) --&gt; Header, FST<sup>NumSuggestFields</sup>, Footer</li>
 *   <li>Header --&gt; {@link CodecUtil#writeHeader CodecHeader}</li>
 *   <!-- TODO: should the FST output be mentioned at all? -->
 *   <li>FST --&gt; {@link FST FST&lt;Long, BytesRef&gt;}</li>
 *   <li>Footer --&gt; {@link CodecUtil#writeFooter CodecFooter}</li>
 * </ul>
 * <p>Notes:</p>
 * <ul>
 *   <li>Header is a {@link CodecUtil#writeHeader CodecHeader} storing the version information
 *     for the Completion implementation.</li>
 *   <li>FST maps all analyzed forms to surface forms of a SuggestField</li>
 * </ul>
 * <a name="Completionindex"></a>
 * <h3>Completion Index</h3>
 * <p>The .cmp file contains an index into the completion dictionary, so that it can be
 * accessed randomly.</p>
 * <ul>
 *   <li>CompletionIndex (.cmp) --&gt; Header, NumSuggestFields, Entry<sup>NumSuggestFields</sup>, Footer</li>
 *   <li>Header --&gt; {@link CodecUtil#writeHeader CodecHeader}</li>
 *   <li>NumSuggestFields --&gt; {@link DataOutput#writeVInt Uint32}</li>
 *   <li>Entry --&gt; FieldNumber, CompletionDictionaryOffset, MinWeight, MaxWeight, Type</li>
 *   <li>FieldNumber --&gt; {@link DataOutput#writeVInt Uint32}</li>
 *   <li>CompletionDictionaryOffset --&gt; {@link DataOutput#writeVLong  Uint64}</li>
 *   <li>MinWeight --&gt; {@link DataOutput#writeVLong  Uint64}</li>
 *   <li>MaxWeight --&gt; {@link DataOutput#writeVLong  Uint64}</li>
 *   <li>Type --&gt; {@link DataOutput#writeByte  Byte}</li>
 *   <li>Footer --&gt; {@link CodecUtil#writeFooter CodecFooter}</li>
 * </ul>
 * <p>Notes:</p>
 * <ul>
 *   <li>Header is a {@link CodecUtil#writeHeader CodecHeader} storing the version information
 *     for the Completion implementation.</li>
 *   <li>NumSuggestFields is the number of suggest fields indexed</li>
 *   <li>FieldNumber is the fields number from {@link FieldInfos}. (.fnm)</li>
 *   <li>CompletionDictionaryOffset is the file offset of a field's FST in CompletionDictionary (.lkp)</li>
 *   <li>MinWeight and MaxWeight are the global minimum and maximum weight for the field</li>
 *   <li>Type indicates if the suggester has context or not</li>
 * </ul>
 *
 * @lucene.experimental
 */
public abstract class CompletionPostingsFormat extends PostingsFormat {

  static final String CODEC_NAME = "completion";
  static final int COMPLETION_CODEC_VERSION = 1;
  static final int COMPLETION_VERSION_CURRENT = COMPLETION_CODEC_VERSION;
  static final String INDEX_EXTENSION = "cmp";
  static final String DICT_EXTENSION = "lkp";

  /**
   * Used only by core Lucene at read-time via Service Provider instantiation
   */
  public CompletionPostingsFormat() {
    super(CODEC_NAME);
  }

  /**
   * Concrete implementation should specify the delegating postings format
   */
  protected abstract PostingsFormat delegatePostingsFormat();

  @Override
  public FieldsConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
    PostingsFormat delegatePostingsFormat = delegatePostingsFormat();
    if (delegatePostingsFormat == null) {
      throw new UnsupportedOperationException("Error - " + getClass().getName()
          + " has been constructed without a choice of PostingsFormat");
    }
    return new CompletionFieldsConsumer(delegatePostingsFormat, state);
  }

  @Override
  public FieldsProducer fieldsProducer(SegmentReadState state) throws IOException {
    return new CompletionFieldsProducer(state);
  }
}
