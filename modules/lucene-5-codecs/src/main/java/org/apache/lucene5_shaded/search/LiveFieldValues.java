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
package org.apache.lucene5_shaded.search;


import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** Tracks live field values across NRT reader reopens.
 *  This holds a map for all updated ids since
 *  the last reader reopen.  Once the NRT reader is reopened,
 *  it prunes the map.  This means you must reopen your NRT
 *  reader periodically otherwise the RAM consumption of
 *  this class will grow unbounded!
 *
 *  <p>NOTE: you must ensure the same id is never updated at
 *  the same time by two threads, because in this case you
 *  cannot in general know which thread "won". */

// TODO: should this class handle deletions better...?
public abstract class LiveFieldValues<S,T> implements ReferenceManager.RefreshListener, Closeable {

  private volatile Map<String,T> current = new ConcurrentHashMap<>();
  private volatile Map<String,T> old = new ConcurrentHashMap<>();
  private final ReferenceManager<S> mgr;
  private final T missingValue;

  /** The missingValue must be non-null. */
  public LiveFieldValues(ReferenceManager<S> mgr, T missingValue) {
    this.missingValue = missingValue;
    this.mgr = mgr;
    mgr.addListener(this);
  }

  @Override
  public void close() {
    mgr.removeListener(this);
  }

  @Override
  public void beforeRefresh() throws IOException {
    old = current;
    // Start sending all updates after this point to the new
    // map.  While reopen is running, any lookup will first
    // try this new map, then fallback to old, then to the
    // current searcher:
    current = new ConcurrentHashMap<>();
  }

  @Override
  public void afterRefresh(boolean didRefresh) throws IOException {
    // Now drop all the old values because they are now
    // visible via the searcher that was just opened; if
    // didRefresh is false, it's possible old has some
    // entries in it, which is fine: it means they were
    // actually already included in the previously opened
    // reader.  So we can safely clear old here:
    old = new ConcurrentHashMap<>();
  }

  /** Call this after you've successfully added a document
   *  to the index, to record what value you just set the
   *  field to. */
  public void add(String id, T value) {
    current.put(id, value);
  }

  /** Call this after you've successfully deleted a document
   *  from the index. */
  public void delete(String id) {
    current.put(id, missingValue);
  }

  /** Returns the [approximate] number of id/value pairs
   *  buffered in RAM. */
  public int size() {
    return current.size() + old.size();
  }

  /** Returns the current value for this id, or null if the
   *  id isn't in the index or was deleted. */
  public T get(String id) throws IOException {
    // First try to get the "live" value:
    T value = current.get(id);
    if (value == missingValue) {
      // Deleted but the deletion is not yet reflected in
      // the reader:
      return null;
    } else if (value != null) {
      return value;
    } else {
      value = old.get(id);
      if (value == missingValue) {
        // Deleted but the deletion is not yet reflected in
        // the reader:
        return null;
      } else if (value != null) {
        return value;
      } else {
        // It either does not exist in the index, or, it was
        // already flushed & NRT reader was opened on the
        // segment, so fallback to current searcher:
        S s = mgr.acquire();
        try {
          return lookupFromSearcher(s, id);
        } finally {
          mgr.release(s);
        }
      }
    }
  }

  /** This is called when the id/value was already flushed and opened
   *  in an NRT IndexSearcher.  You must implement this to
   *  go look up the value (eg, via doc values, field cache,
   *  stored fields, etc.). */
  protected abstract T lookupFromSearcher(S s, String id) throws IOException;
}

