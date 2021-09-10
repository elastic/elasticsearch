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
package org.apache.lucene5_shaded.store;


/**
 * IOContext holds additional details on the merge/search context. A IOContext
 * object can never be initialized as null as passed as a parameter to either
 * {@link Directory#openInput(String, IOContext)} or
 * {@link Directory#createOutput(String, IOContext)}
 */
public class IOContext {

  /**
   * Context is a enumerator which specifies the context in which the Directory
   * is being used for.
   */
  public enum Context {
    MERGE, READ, FLUSH, DEFAULT
  };

  /**
   * An object of a enumerator Context type
   */
  public final Context context;

  public final MergeInfo mergeInfo;

  public final FlushInfo flushInfo;

  public final boolean readOnce;

  public static final IOContext DEFAULT = new IOContext(Context.DEFAULT);

  public static final IOContext READONCE = new IOContext(true);

  public static final IOContext READ = new IOContext(false);

  public IOContext() {
    this(false);
  }

  public IOContext(FlushInfo flushInfo) {
    assert flushInfo != null;
    this.context = Context.FLUSH;
    this.mergeInfo = null;
    this.readOnce = false;
    this.flushInfo = flushInfo;
  }

  public IOContext(Context context) {
    this(context, null);
  }

  private IOContext(boolean readOnce) {
    this.context = Context.READ;
    this.mergeInfo = null;
    this.readOnce = readOnce;
    this.flushInfo = null;
  }

  public IOContext(MergeInfo mergeInfo) {
    this(Context.MERGE, mergeInfo);
  }
  
  private IOContext(Context context, MergeInfo mergeInfo) {
    assert context != Context.MERGE || mergeInfo != null : "MergeInfo must not be null if context is MERGE";
    assert context != Context.FLUSH : "Use IOContext(FlushInfo) to create a FLUSH IOContext";
    this.context = context;
    this.readOnce = false;
    this.mergeInfo = mergeInfo;
    this.flushInfo = null;
  }
  
  /**
   * This constructor is used to initialize a {@link IOContext} instance with a new value for the readOnce variable. 
   * @param ctxt {@link IOContext} object whose information is used to create the new instance except the readOnce variable.
   * @param readOnce The new {@link IOContext} object will use this value for readOnce. 
   */
  public IOContext(IOContext ctxt, boolean readOnce) {
    this.context = ctxt.context;
    this.mergeInfo = ctxt.mergeInfo;
    this.flushInfo = ctxt.flushInfo;
    this.readOnce = readOnce;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((context == null) ? 0 : context.hashCode());
    result = prime * result + ((flushInfo == null) ? 0 : flushInfo.hashCode());
    result = prime * result + ((mergeInfo == null) ? 0 : mergeInfo.hashCode());
    result = prime * result + (readOnce ? 1231 : 1237);
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    IOContext other = (IOContext) obj;
    if (context != other.context)
      return false;
    if (flushInfo == null) {
      if (other.flushInfo != null)
        return false;
    } else if (!flushInfo.equals(other.flushInfo))
      return false;
    if (mergeInfo == null) {
      if (other.mergeInfo != null)
        return false;
    } else if (!mergeInfo.equals(other.mergeInfo))
      return false;
    if (readOnce != other.readOnce)
      return false;
    return true;
  }

  @Override
  public String toString() {
    return "IOContext [context=" + context + ", mergeInfo=" + mergeInfo
        + ", flushInfo=" + flushInfo + ", readOnce=" + readOnce + "]";
  }

}