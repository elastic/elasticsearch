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


/**
 * A {@link MergeScheduler} which never executes any merges. It is also a
 * singleton and can be accessed through {@link NoMergeScheduler#INSTANCE}. Use
 * it if you want to prevent an {@link IndexWriter} from ever executing merges,
 * regardless of the {@link MergePolicy} used. Note that you can achieve the
 * same thing by using {@link NoMergePolicy}, however with
 * {@link NoMergeScheduler} you also ensure that no unnecessary code of any
 * {@link MergeScheduler} implementation is ever executed. Hence it is
 * recommended to use both if you want to disable merges from ever happening.
 */
public final class NoMergeScheduler extends MergeScheduler {

  /** The single instance of {@link NoMergeScheduler} */
  public static final MergeScheduler INSTANCE = new NoMergeScheduler();

  private NoMergeScheduler() {
    // prevent instantiation
  }

  @Override
  public void close() {}

  @Override
  public void merge(IndexWriter writer, MergeTrigger trigger, boolean newMergesFound) {}

  @Override
  public MergeScheduler clone() {
    return this;
  }
}
