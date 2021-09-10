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
package org.apache.lucene5_shaded.codecs;


/**
 * A codec that forwards all its method calls to another codec.
 * <p>
 * Extend this class when you need to reuse the functionality of an existing
 * codec. For example, if you want to build a codec that redefines LuceneMN's
 * {@link LiveDocsFormat}:
 * <pre class="prettyprint">
 *   public final class CustomCodec extends FilterCodec {
 *
 *     public CustomCodec() {
 *       super("CustomCodec", new LuceneMNCodec());
 *     }
 *
 *     public LiveDocsFormat liveDocsFormat() {
 *       return new CustomLiveDocsFormat();
 *     }
 *
 *   }
 * </pre>
 * 
 * <p><em>Please note:</em> Don't call {@link Codec#forName} from
 * the no-arg constructor of your own codec. When the SPI framework
 * loads your own Codec as SPI component, SPI has not yet fully initialized!
 * If you want to extend another Codec, instantiate it directly by calling
 * its constructor.
 * 
 * @lucene.experimental
 */
public abstract class FilterCodec extends Codec {

  /** The codec to filter. */
  protected final Codec delegate;
  
  /** Sole constructor. When subclassing this codec,
   * create a no-arg ctor and pass the delegate codec
   * and a unique name to this ctor.
   */
  protected FilterCodec(String name, Codec delegate) {
    super(name);
    this.delegate = delegate;
  }

  @Override
  public DocValuesFormat docValuesFormat() {
    return delegate.docValuesFormat();
  }

  @Override
  public FieldInfosFormat fieldInfosFormat() {
    return delegate.fieldInfosFormat();
  }

  @Override
  public LiveDocsFormat liveDocsFormat() {
    return delegate.liveDocsFormat();
  }

  @Override
  public NormsFormat normsFormat() {
    return delegate.normsFormat();
  }

  @Override
  public PostingsFormat postingsFormat() {
    return delegate.postingsFormat();
  }

  @Override
  public SegmentInfoFormat segmentInfoFormat() {
    return delegate.segmentInfoFormat();
  }

  @Override
  public StoredFieldsFormat storedFieldsFormat() {
    return delegate.storedFieldsFormat();
  }

  @Override
  public TermVectorsFormat termVectorsFormat() {
    return delegate.termVectorsFormat();
  }

  @Override
  public CompoundFormat compoundFormat() {
    return delegate.compoundFormat();
  }
}
