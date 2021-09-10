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


import java.io.IOException;

import org.apache.lucene5_shaded.index.Term;
import org.apache.lucene5_shaded.index.Terms;
import org.apache.lucene5_shaded.index.TermsEnum;
import org.apache.lucene5_shaded.util.AttributeSource;
import org.apache.lucene5_shaded.util.ToStringUtils;
import org.apache.lucene5_shaded.util.automaton.Automaton;
import org.apache.lucene5_shaded.util.automaton.CompiledAutomaton;
import org.apache.lucene5_shaded.util.automaton.Operations;

/**
 * A {@link Query} that will match terms against a finite-state machine.
 * <p>
 * This query will match documents that contain terms accepted by a given
 * finite-state machine. The automaton can be constructed with the
 * {@link org.apache.lucene5_shaded.util.automaton} API. Alternatively, it can be
 * created from a regular expression with {@link RegexpQuery} or from
 * the standard Lucene wildcard syntax with {@link WildcardQuery}.
 * </p>
 * <p>
 * When the query is executed, it will create an equivalent DFA of the
 * finite-state machine, and will enumerate the term dictionary in an
 * intelligent way to reduce the number of comparisons. For example: the regular
 * expression of <code>[dl]og?</code> will make approximately four comparisons:
 * do, dog, lo, and log.
 * </p>
 * @lucene.experimental
 */
public class AutomatonQuery extends MultiTermQuery {
  /** the automaton to match index terms against */
  protected final Automaton automaton;
  protected final CompiledAutomaton compiled;
  /** term containing the field, and possibly some pattern structure */
  protected final Term term;

  /**
   * Create a new AutomatonQuery from an {@link Automaton}.
   * 
   * @param term Term containing field and possibly some pattern structure. The
   *        term text is ignored.
   * @param automaton Automaton to run, terms that are accepted are considered a
   *        match.
   */
  public AutomatonQuery(final Term term, Automaton automaton) {
    this(term, automaton, Operations.DEFAULT_MAX_DETERMINIZED_STATES);
  }

  /**
   * Create a new AutomatonQuery from an {@link Automaton}.
   * 
   * @param term Term containing field and possibly some pattern structure. The
   *        term text is ignored.
   * @param automaton Automaton to run, terms that are accepted are considered a
   *        match.
   * @param maxDeterminizedStates maximum number of states in the resulting
   *   automata.  If the automata would need more than this many states
   *   TooComplextToDeterminizeException is thrown.  Higher number require more
   *   space but can process more complex automata.
   */
  public AutomatonQuery(final Term term, Automaton automaton, int maxDeterminizedStates) {
    this(term, automaton, maxDeterminizedStates, false);
  }

  /**
   * Create a new AutomatonQuery from an {@link Automaton}.
   * 
   * @param term Term containing field and possibly some pattern structure. The
   *        term text is ignored.
   * @param automaton Automaton to run, terms that are accepted are considered a
   *        match.
   * @param maxDeterminizedStates maximum number of states in the resulting
   *   automata.  If the automata would need more than this many states
   *   TooComplextToDeterminizeException is thrown.  Higher number require more
   *   space but can process more complex automata.
   * @param isBinary if true, this automaton is already binary and
   *   will not go through the UTF32ToUTF8 conversion
   */
  public AutomatonQuery(final Term term, Automaton automaton, int maxDeterminizedStates, boolean isBinary) {
    super(term.field());
    this.term = term;
    this.automaton = automaton;
    // TODO: we could take isFinite too, to save a bit of CPU in CompiledAutomaton ctor?:
    this.compiled = new CompiledAutomaton(automaton, null, true, maxDeterminizedStates, isBinary);
  }

  @Override
  protected TermsEnum getTermsEnum(Terms terms, AttributeSource atts) throws IOException {
    return compiled.getTermsEnum(terms);
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = prime * result + compiled.hashCode();
    result = prime * result + ((term == null) ? 0 : term.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (!super.equals(obj))
      return false;
    if (getClass() != obj.getClass())
      return false;
    AutomatonQuery other = (AutomatonQuery) obj;
    if (!compiled.equals(other.compiled))
      return false;
    if (term == null) {
      if (other.term != null)
        return false;
    } else if (!term.equals(other.term))
      return false;
    return true;
  }

  @Override
  public String toString(String field) {
    StringBuilder buffer = new StringBuilder();
    if (!term.field().equals(field)) {
      buffer.append(term.field());
      buffer.append(":");
    }
    buffer.append(getClass().getSimpleName());
    buffer.append(" {");
    buffer.append('\n');
    buffer.append(automaton.toString());
    buffer.append("}");
    buffer.append(ToStringUtils.boost(getBoost()));
    return buffer.toString();
  }
  
  /** Returns the automaton used to create this query */
  public Automaton getAutomaton() {
    return automaton;
  }
}
