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


import java.io.IOException;
import java.util.ServiceLoader;
import java.util.Set;

import org.apache.lucene5_shaded.index.SegmentReadState;
import org.apache.lucene5_shaded.index.SegmentWriteState;
import org.apache.lucene5_shaded.util.NamedSPILoader;

/** 
 * Encodes/decodes per-document values.
 * <p>
 * Note, when extending this class, the name ({@link #getName}) may
 * written into the index in certain configurations. In order for the segment 
 * to be read, the name must resolve to your implementation via {@link #forName(String)}.
 * This method uses Java's 
 * {@link ServiceLoader Service Provider Interface} (SPI) to resolve format names.
 * <p>
 * If you implement your own format, make sure that it has a no-arg constructor
 * so SPI can load it.
 * @see ServiceLoader
 * @lucene.experimental */
public abstract class DocValuesFormat implements NamedSPILoader.NamedSPI {
  
  /**
   * This static holder class prevents classloading deadlock by delaying
   * init of doc values formats until needed.
   */
  private static final class Holder {
    private static final NamedSPILoader<DocValuesFormat> LOADER = new NamedSPILoader<>(DocValuesFormat.class);
    
    private Holder() {}
    
    static NamedSPILoader<DocValuesFormat> getLoader() {
      if (LOADER == null) {
        throw new IllegalStateException("You tried to lookup a DocValuesFormat by name before all formats could be initialized. "+
          "This likely happens if you call DocValuesFormat#forName from a DocValuesFormat's ctor.");
      }
      return LOADER;
    }
  }
  
  /** Unique name that's used to retrieve this format when
   *  reading the index.
   */
  private final String name;

  /**
   * Creates a new docvalues format.
   * <p>
   * The provided name will be written into the index segment in some configurations
   * (such as when using {@code PerFieldDocValuesFormat}): in such configurations,
   * for the segment to be read this class should be registered with Java's
   * SPI mechanism (registered in META-INF/ of your jar file, etc).
   * @param name must be all ascii alphanumeric, and less than 128 characters in length.
   */
  protected DocValuesFormat(String name) {
    NamedSPILoader.checkServiceName(name);
    this.name = name;
  }

  /** Returns a {@link DocValuesConsumer} to write docvalues to the
   *  index. */
  public abstract DocValuesConsumer fieldsConsumer(SegmentWriteState state) throws IOException;

  /** 
   * Returns a {@link DocValuesProducer} to read docvalues from the index. 
   * <p>
   * NOTE: by the time this call returns, it must hold open any files it will 
   * need to use; else, those files may be deleted. Additionally, required files 
   * may be deleted during the execution of this call before there is a chance 
   * to open them. Under these circumstances an IOException should be thrown by 
   * the implementation. IOExceptions are expected and will automatically cause 
   * a retry of the segment opening logic with the newly revised segments.
   */
  public abstract DocValuesProducer fieldsProducer(SegmentReadState state) throws IOException;

  @Override
  public final String getName() {
    return name;
  }
  
  @Override
  public String toString() {
    return "DocValuesFormat(name=" + name + ")";
  }
  
  /** looks up a format by name */
  public static DocValuesFormat forName(String name) {
    return Holder.getLoader().lookup(name);
  }
  
  /** returns a list of all available format names */
  public static Set<String> availableDocValuesFormats() {
    return Holder.getLoader().availableServices();
  }
  
  /** 
   * Reloads the DocValues format list from the given {@link ClassLoader}.
   * Changes to the docvalues formats are visible after the method ends, all
   * iterators ({@link #availableDocValuesFormats()},...) stay consistent. 
   * 
   * <p><b>NOTE:</b> Only new docvalues formats are added, existing ones are
   * never removed or replaced.
   * 
   * <p><em>This method is expensive and should only be called for discovery
   * of new docvalues formats on the given classpath/classloader!</em>
   */
  public static void reloadDocValuesFormats(ClassLoader classloader) {
    Holder.getLoader().reload(classloader);
  }
}
