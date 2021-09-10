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

import org.apache.lucene5_shaded.index.SegmentReadState;
import org.apache.lucene5_shaded.index.SegmentWriteState;

/**
 * Encodes/decodes per-document score normalization values.
 */
public abstract class NormsFormat {
  /** Sole constructor. (For invocation by subclass 
   *  constructors, typically implicit.) */
  protected NormsFormat() {
  }

  /** Returns a {@link NormsConsumer} to write norms to the
   *  index. */
  public abstract NormsConsumer normsConsumer(SegmentWriteState state) throws IOException;

  /** 
   * Returns a {@link NormsProducer} to read norms from the index. 
   * <p>
   * NOTE: by the time this call returns, it must hold open any files it will 
   * need to use; else, those files may be deleted. Additionally, required files 
   * may be deleted during the execution of this call before there is a chance 
   * to open them. Under these circumstances an IOException should be thrown by 
   * the implementation. IOExceptions are expected and will automatically cause 
   * a retry of the segment opening logic with the newly revised segments.
   */
  public abstract NormsProducer normsProducer(SegmentReadState state) throws IOException;
}
