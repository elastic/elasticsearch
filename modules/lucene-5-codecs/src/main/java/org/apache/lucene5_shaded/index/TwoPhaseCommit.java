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

import java.io.IOException;


/**
 * An interface for implementations that support 2-phase commit. You can use
 * {@link TwoPhaseCommitTool} to execute a 2-phase commit algorithm over several
 * {@link TwoPhaseCommit}s.
 * 
 * @lucene.experimental
 */
public interface TwoPhaseCommit {

  /**
   * The first stage of a 2-phase commit. Implementations should do as much work
   * as possible in this method, but avoid actual committing changes. If the
   * 2-phase commit fails, {@link #rollback()} is called to discard all changes
   * since last successful commit.
   */
  public void prepareCommit() throws IOException;

  /**
   * The second phase of a 2-phase commit. Implementations should ideally do
   * very little work in this method (following {@link #prepareCommit()}, and
   * after it returns, the caller can assume that the changes were successfully
   * committed to the underlying storage.
   */
  public void commit() throws IOException;

  /**
   * Discards any changes that have occurred since the last commit. In a 2-phase
   * commit algorithm, where one of the objects failed to {@link #commit()} or
   * {@link #prepareCommit()}, this method is used to roll all other objects
   * back to their previous state.
   */
  public void rollback() throws IOException;

}
