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


import java.util.Objects;
import java.util.Set;
import java.util.ServiceLoader; // javadocs

import org.apache.lucene5_shaded.index.IndexWriterConfig; // javadocs
import org.apache.lucene5_shaded.util.NamedSPILoader;

/**
 * Encodes/decodes an inverted index segment.
 * <p>
 * Note, when extending this class, the name ({@link #getName}) is 
 * written into the index. In order for the segment to be read, the
 * name must resolve to your implementation via {@link #forName(String)}.
 * This method uses Java's 
 * {@link ServiceLoader Service Provider Interface} (SPI) to resolve codec names.
 * <p>
 * If you implement your own codec, make sure that it has a no-arg constructor
 * so SPI can load it.
 * @see ServiceLoader
 */
public abstract class Codec implements NamedSPILoader.NamedSPI {

  /**
   * This static holder class prevents classloading deadlock by delaying
   * init of default codecs and available codecs until needed.
   */
  private static final class Holder {
    private static final NamedSPILoader<Codec> LOADER = new NamedSPILoader<>(Codec.class);
    
    private Holder() {}
    
    static NamedSPILoader<Codec> getLoader() {
      if (LOADER == null) {
        throw new IllegalStateException("You tried to lookup a Codec by name before all Codecs could be initialized. "+
          "This likely happens if you call Codec#forName from a Codec's ctor.");
      }
      return LOADER;
    }
    
    // TODO: should we use this, or maybe a system property is better?
    static Codec defaultCodec = LOADER.lookup("Lucene54");
  }

  private final String name;

  /**
   * Creates a new codec.
   * <p>
   * The provided name will be written into the index segment: in order to
   * for the segment to be read this class should be registered with Java's
   * SPI mechanism (registered in META-INF/ of your jar file, etc).
   * @param name must be all ascii alphanumeric, and less than 128 characters in length.
   */
  protected Codec(String name) {
    NamedSPILoader.checkServiceName(name);
    this.name = name;
  }
  
  /** Returns this codec's name */
  @Override
  public final String getName() {
    return name;
  }
  
  /** Encodes/decodes postings */
  public abstract PostingsFormat postingsFormat();

  /** Encodes/decodes docvalues */
  public abstract DocValuesFormat docValuesFormat();
  
  /** Encodes/decodes stored fields */
  public abstract StoredFieldsFormat storedFieldsFormat();
  
  /** Encodes/decodes term vectors */
  public abstract TermVectorsFormat termVectorsFormat();
  
  /** Encodes/decodes field infos file */
  public abstract FieldInfosFormat fieldInfosFormat();
  
  /** Encodes/decodes segment info file */
  public abstract SegmentInfoFormat segmentInfoFormat();
  
  /** Encodes/decodes document normalization values */
  public abstract NormsFormat normsFormat();

  /** Encodes/decodes live docs */
  public abstract LiveDocsFormat liveDocsFormat();
  
  /** Encodes/decodes compound files */
  public abstract CompoundFormat compoundFormat();
  
  /** looks up a codec by name */
  public static Codec forName(String name) {
    return Holder.getLoader().lookup(name);
  }
  
  /** returns a list of all available codec names */
  public static Set<String> availableCodecs() {
    return Holder.getLoader().availableServices();
  }
  
  /** 
   * Reloads the codec list from the given {@link ClassLoader}.
   * Changes to the codecs are visible after the method ends, all
   * iterators ({@link #availableCodecs()},...) stay consistent. 
   * 
   * <p><b>NOTE:</b> Only new codecs are added, existing ones are
   * never removed or replaced.
   * 
   * <p><em>This method is expensive and should only be called for discovery
   * of new codecs on the given classpath/classloader!</em>
   */
  public static void reloadCodecs(ClassLoader classloader) {
    Holder.getLoader().reload(classloader);
  }
    
  /** expert: returns the default codec used for newly created
   *  {@link IndexWriterConfig}s.
   */
  public static Codec getDefault() {
    if (Holder.defaultCodec == null) {
      throw new IllegalStateException("You tried to lookup the default Codec before all Codecs could be initialized. "+
        "This likely happens if you try to get it from a Codec's ctor.");
    }
    return Holder.defaultCodec;
  }
  
  /** expert: sets the default codec used for newly created
   *  {@link IndexWriterConfig}s.
   */
  public static void setDefault(Codec codec) {
    Holder.defaultCodec = Objects.requireNonNull(codec);
  }

  /**
   * returns the codec's name. Subclasses can override to provide
   * more detail (such as parameters).
   */
  @Override
  public String toString() {
    return name;
  }
}
