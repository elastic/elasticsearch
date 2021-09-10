/*
 * dk.brics.automaton
 * 
 * Copyright (c) 2001-2009 Anders Moeller
 * All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 * 3. The name of the author may not be used to endorse or promote products
 *    derived from this software without specific prior written permission.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE AUTHOR ``AS IS'' AND ANY EXPRESS OR
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
 * OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
 * IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY DIRECT, INDIRECT,
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT
 * NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF
 * THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package org.apache.lucene5_shaded.util.automaton;

/**
 * Pair of states.
 * 
 * @lucene.experimental
 */
public class StatePair {
  int s;
  int s1;
  int s2;
  
  StatePair(int s, int s1, int s2) {
    this.s = s;
    this.s1 = s1;
    this.s2 = s2;
  }
  
  /**
   * Constructs a new state pair.
   * 
   * @param s1 first state
   * @param s2 second state
   */
  public StatePair(int s1, int s2) {
    this.s1 = s1;
    this.s2 = s2;
    this.s = -1;
  }
  
  /**
   * Checks for equality.
   * 
   * @param obj object to compare with
   * @return true if <tt>obj</tt> represents the same pair of states as this
   *         pair
   */
  @Override
  public boolean equals(Object obj) {
    if (obj instanceof StatePair) {
      StatePair p = (StatePair) obj;
      return p.s1 == s1 && p.s2 == s2;
    } else return false;
  }
  
  /**
   * Returns hash code.
   * 
   * @return hash code
   */
  @Override
  public int hashCode() {
    return s1 ^ s2;
  }

  @Override
  public String toString() {
    return "StatePair(s1=" + s1 + " s2=" + s2 + ")";
  }
}
