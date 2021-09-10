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

import org.apache.lucene5_shaded.util.BytesRef;
import org.apache.lucene5_shaded.util.AttributeSource;

/**
 * Abstract class for enumerating a subset of all terms. 
 * 
 * <p>Term enumerations are always ordered by
 * {@link BytesRef#compareTo}.  Each term in the enumeration is
 * greater than all that precede it.</p>
 * <p><em>Please note:</em> Consumers of this enum cannot
 * call {@code seek()}, it is forward only; it throws
 * {@link UnsupportedOperationException} when a seeking method
 * is called.
 */
public abstract class FilteredTermsEnum extends TermsEnum {

  private BytesRef initialSeekTerm;
  private boolean doSeek;

  /** Which term the enum is currently positioned to. */
  protected BytesRef actualTerm;

  /** The delegate {@link TermsEnum}. */
  protected final TermsEnum tenum;

  /** Return value, if term should be accepted or the iteration should
   * {@code END}. The {@code *_SEEK} values denote, that after handling the current term
   * the enum should call {@link #nextSeekTerm} and step forward.
   * @see #accept(BytesRef)
   */
  protected static enum AcceptStatus {
    /** Accept the term and position the enum at the next term. */
    YES, 
    /** Accept the term and advance ({@link FilteredTermsEnum#nextSeekTerm(BytesRef)})
     * to the next term. */
    YES_AND_SEEK, 
    /** Reject the term and position the enum at the next term. */
    NO, 
    /** Reject the term and advance ({@link FilteredTermsEnum#nextSeekTerm(BytesRef)})
     * to the next term. */
    NO_AND_SEEK, 
    /** Reject the term and stop enumerating. */
    END
  };
  
  /** Return if term is accepted, not accepted or the iteration should ended
   * (and possibly seek).
   */
  protected abstract AcceptStatus accept(BytesRef term) throws IOException;

  /**
   * Creates a filtered {@link TermsEnum} on a terms enum.
   * @param tenum the terms enumeration to filter.
   */
  public FilteredTermsEnum(final TermsEnum tenum) {
    this(tenum, true);
  }

  /**
   * Creates a filtered {@link TermsEnum} on a terms enum.
   * @param tenum the terms enumeration to filter.
   */
  public FilteredTermsEnum(final TermsEnum tenum, final boolean startWithSeek) {
    assert tenum != null;
    this.tenum = tenum;
    doSeek = startWithSeek;
  }

  /**
   * Use this method to set the initial {@link BytesRef}
   * to seek before iterating. This is a convenience method for
   * subclasses that do not override {@link #nextSeekTerm}.
   * If the initial seek term is {@code null} (default),
   * the enum is empty.
   * <P>You can only use this method, if you keep the default
   * implementation of {@link #nextSeekTerm}.
   */
  protected final void setInitialSeekTerm(BytesRef term) {
    this.initialSeekTerm = term;
  }
  
  /** On the first call to {@link #next} or if {@link #accept} returns
   * {@link AcceptStatus#YES_AND_SEEK} or {@link AcceptStatus#NO_AND_SEEK},
   * this method will be called to eventually seek the underlying TermsEnum
   * to a new position.
   * On the first call, {@code currentTerm} will be {@code null}, later
   * calls will provide the term the underlying enum is positioned at.
   * This method returns per default only one time the initial seek term
   * and then {@code null}, so no repositioning is ever done.
   * <p>Override this method, if you want a more sophisticated TermsEnum,
   * that repositions the iterator during enumeration.
   * If this method always returns {@code null} the enum is empty.
   * <p><em>Please note:</em> This method should always provide a greater term
   * than the last enumerated term, else the behaviour of this enum
   * violates the contract for TermsEnums.
   */
  protected BytesRef nextSeekTerm(final BytesRef currentTerm) throws IOException {
    final BytesRef t = initialSeekTerm;
    initialSeekTerm = null;
    return t;
  }

  /**
   * Returns the related attributes, the returned {@link AttributeSource}
   * is shared with the delegate {@code TermsEnum}.
   */
  @Override
  public AttributeSource attributes() {
    return tenum.attributes();
  }
  
  @Override
  public BytesRef term() throws IOException {
    return tenum.term();
  }

  @Override
  public int docFreq() throws IOException {
    return tenum.docFreq();
  }

  @Override
  public long totalTermFreq() throws IOException {
    return tenum.totalTermFreq();
  }

  /** This enum does not support seeking!
   * @throws UnsupportedOperationException In general, subclasses do not
   *         support seeking.
   */
  @Override
  public boolean seekExact(BytesRef term) throws IOException {
    throw new UnsupportedOperationException(getClass().getName()+" does not support seeking");
  }

  /** This enum does not support seeking!
   * @throws UnsupportedOperationException In general, subclasses do not
   *         support seeking.
   */
  @Override
  public SeekStatus seekCeil(BytesRef term) throws IOException {
    throw new UnsupportedOperationException(getClass().getName()+" does not support seeking");
  }

  /** This enum does not support seeking!
   * @throws UnsupportedOperationException In general, subclasses do not
   *         support seeking.
   */
  @Override
  public void seekExact(long ord) throws IOException {
    throw new UnsupportedOperationException(getClass().getName()+" does not support seeking");
  }

  @Override
  public long ord() throws IOException {
    return tenum.ord();
  }

  @Override
  public PostingsEnum postings(PostingsEnum reuse, int flags) throws IOException {
    return tenum.postings(reuse, flags);
  }
  
  /** This enum does not support seeking!
   * @throws UnsupportedOperationException In general, subclasses do not
   *         support seeking.
   */
  @Override
  public void seekExact(BytesRef term, TermState state) throws IOException {
    throw new UnsupportedOperationException(getClass().getName()+" does not support seeking");
  }
  
  /**
   * Returns the filtered enums term state 
   */
  @Override
  public TermState termState() throws IOException {
    assert tenum != null;
    return tenum.termState();
  }

  @SuppressWarnings("fallthrough")
  @Override
  public BytesRef next() throws IOException {
    //System.out.println("FTE.next doSeek=" + doSeek);
    //new Throwable().printStackTrace(System.out);
    for (;;) {
      // Seek or forward the iterator
      if (doSeek) {
        doSeek = false;
        final BytesRef t = nextSeekTerm(actualTerm);
        //System.out.println("  seek to t=" + (t == null ? "null" : t.utf8ToString()) + " tenum=" + tenum);
        // Make sure we always seek forward:
        assert actualTerm == null || t == null || t.compareTo(actualTerm) > 0: "curTerm=" + actualTerm + " seekTerm=" + t;
        if (t == null || tenum.seekCeil(t) == SeekStatus.END) {
          // no more terms to seek to or enum exhausted
          //System.out.println("  return null");
          return null;
        }
        actualTerm = tenum.term();
        //System.out.println("  got term=" + actualTerm.utf8ToString());
      } else {
        actualTerm = tenum.next();
        if (actualTerm == null) {
          // enum exhausted
          return null;
        }
      }
      
      // check if term is accepted
      switch (accept(actualTerm)) {
        case YES_AND_SEEK:
          doSeek = true;
          // term accepted, but we need to seek so fall-through
        case YES:
          // term accepted
          return actualTerm;
        case NO_AND_SEEK:
          // invalid term, seek next time
          doSeek = true;
          break;
        case END:
          // we are supposed to end the enum
          return null;
        // NO: we just fall through and iterate again
      }
    }
  }

}
