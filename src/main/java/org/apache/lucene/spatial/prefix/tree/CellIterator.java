/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.lucene.spatial.prefix.tree;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * An Iterator of SpatialPrefixTree Cells. The order is always sorted without duplicates.
 *
 * @lucene.experimental
 *
 * NOTE: Will be removed upon commit of LUCENE-6422
 */
public abstract class CellIterator implements Iterator<Cell> {

  //note: nextCell or thisCell can be non-null but neither at the same time. That's
  // because they might return the same instance when re-used!

  protected Cell nextCell;//to be returned by next(), and null'ed after
  protected Cell thisCell;//see next() & thisCell(). Should be cleared in hasNext().

  /** Returns the cell last returned from {@link #next()}. It's cleared by hasNext(). */
  public Cell thisCell() {
    assert thisCell != null : "Only call thisCell() after next(), not hasNext()";
    return thisCell;
  }

  // Arguably this belongs here and not on Cell
  //public SpatialRelation getShapeRel()

  /**
   * Gets the next cell that is &gt;= {@code fromCell}, compared using non-leaf bytes. If it returns null then
   * the iterator is exhausted.
   */
  public Cell nextFrom(Cell fromCell) {
    while (true) {
      if (!hasNext())
        return null;
      Cell c = next();//will update thisCell
      if (c.compareToNoLeaf(fromCell) >= 0) {
        return c;
      }
    }
  }

  /** This prevents sub-cells (those underneath the current cell) from being iterated to,
   *  if applicable, otherwise a NO-OP. */
  @Override
  public void remove() {
    assert thisCell != null;
  }

  @Override
  public Cell next() {
    if (nextCell == null) {
      if (!hasNext())
        throw new NoSuchElementException();
    }
    thisCell = nextCell;
    nextCell = null;
    return thisCell;
  }
}
