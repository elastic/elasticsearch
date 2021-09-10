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


import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/** Expert: Describes the score computation for document and query. */
public final class Explanation {

  /**
   * Create a new explanation for a match.
   * @param value       the contribution to the score of the document
   * @param description how {@code value} was computed
   * @param details     sub explanations that contributed to this explanation
   */
  public static Explanation match(float value, String description, Collection<Explanation> details) {
    return new Explanation(true, value, description, details);
  }

  /**
   * Create a new explanation for a match.
   * @param value       the contribution to the score of the document
   * @param description how {@code value} was computed
   * @param details     sub explanations that contributed to this explanation
   */
  public static Explanation match(float value, String description, Explanation... details) {
    return new Explanation(true, value, description, Arrays.asList(details));
  }

  /**
   * Create a new explanation for a document which does not match.
   */
  public static Explanation noMatch(String description, Collection<Explanation> details) {
    return new Explanation(false, 0f, description, details);
  }

  /**
   * Create a new explanation for a document which does not match.
   */
  public static Explanation noMatch(String description, Explanation... details) {
    return new Explanation(false, 0f, description, Arrays.asList(details));
  }

  private final boolean match;                          // whether the document matched
  private final float value;                            // the value of this node
  private final String description;                     // what it represents
  private final List<Explanation> details;              // sub-explanations

  /** Create a new explanation  */
  private Explanation(boolean match, float value, String description, Collection<Explanation> details) {
    this.match = match;
    this.value = value;
    this.description = Objects.requireNonNull(description);
    this.details = Collections.unmodifiableList(new ArrayList<>(details));
    for (Explanation detail : details) {
      Objects.requireNonNull(detail);
    }
  }

  /**
   * Indicates whether or not this Explanation models a match.
   */
  public boolean isMatch() {
    return match;
  }
  
  /** The value assigned to this explanation node. */
  public float getValue() { return value; }

  /** A description of this explanation node. */
  public String getDescription() { return description; }

  private String getSummary() {
    return getValue() + " = " + getDescription();
  }
  
  /** The sub-nodes of this explanation node. */
  public Explanation[] getDetails() {
    return details.toArray(new Explanation[0]);
  }

  /** Render an explanation as text. */
  @Override
  public String toString() {
    return toString(0);
  }

  private String toString(int depth) {
    StringBuilder buffer = new StringBuilder();
    for (int i = 0; i < depth; i++) {
      buffer.append("  ");
    }
    buffer.append(getSummary());
    buffer.append("\n");

    Explanation[] details = getDetails();
    for (int i = 0 ; i < details.length; i++) {
      buffer.append(details[i].toString(depth+1));
    }

    return buffer.toString();
  }


  /** Render an explanation as HTML. */
  public String toHtml() {
    StringBuilder buffer = new StringBuilder();
    buffer.append("<ul>\n");

    buffer.append("<li>");
    buffer.append(getSummary());
    buffer.append("<br />\n");

    Explanation[] details = getDetails();
    for (int i = 0 ; i < details.length; i++) {
      buffer.append(details[i].toHtml());
    }

    buffer.append("</li>\n");
    buffer.append("</ul>\n");

    return buffer.toString();
  }
}
