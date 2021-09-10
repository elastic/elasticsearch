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
package org.apache.lucene5_shaded.index;


import java.io.Closeable;
import java.io.IOException;

import org.apache.lucene5_shaded.util.InfoStream;

/** <p>Expert: {@link IndexWriter} uses an instance
 *  implementing this interface to execute the merges
 *  selected by a {@link MergePolicy}.  The default
 *  MergeScheduler is {@link ConcurrentMergeScheduler}.</p>
 *  <p>Implementers of sub-classes should make sure that {@link #clone()}
 *  returns an independent instance able to work with any {@link IndexWriter}
 *  instance.</p>
 * @lucene.experimental
*/
public abstract class MergeScheduler implements Closeable {

  /** Sole constructor. (For invocation by subclass 
   *  constructors, typically implicit.) */
  protected MergeScheduler() {
  }

  /** Run the merges provided by {@link IndexWriter#getNextMerge()}.
   * @param writer the {@link IndexWriter} to obtain the merges from.
   * @param trigger the {@link MergeTrigger} that caused this merge to happen
   * @param newMergesFound <code>true</code> iff any new merges were found by the caller otherwise <code>false</code>
   * */
  public abstract void merge(IndexWriter writer, MergeTrigger trigger, boolean newMergesFound) throws IOException;

  /** Close this MergeScheduler. */
  @Override
  public abstract void close() throws IOException;

  /** For messages about merge scheduling */
  protected InfoStream infoStream;

  /** IndexWriter calls this on init. */
  final void setInfoStream(InfoStream infoStream) {
    this.infoStream = infoStream;
  }

  /**
   * Returns true if infoStream messages are enabled. This method is usually used in
   * conjunction with {@link #message(String)}:
   * 
   * <pre class="prettyprint">
   * if (verbose()) {
   *   message(&quot;your message&quot;);
   * }
   * </pre>
   */
  protected boolean verbose() {
    return infoStream != null && infoStream.isEnabled("MS");
  }
 
  /**
   * Outputs the given message - this method assumes {@link #verbose()} was
   * called and returned true.
   */
  protected void message(String message) {
    infoStream.message("MS", message);
  }
}
