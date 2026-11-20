/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 *
 * Forked from RE2/J 1.8, see package-info.java.
 *
 * Copyright (c) 2020 The Go Authors. All rights reserved.
 *
 * Use of this source code is governed by a BSD-style
 * license that can be found in the LICENSE file.
 */
package org.elasticsearch.xpack.esql.core.expression.predicate.regex.re2;

/**
 * An exception thrown by the parser if the pattern was invalid.
 *
 * Following {@code java.util.regex.PatternSyntaxException}, this is an unchecked exception.
 */
// ES: an IllegalArgumentException, which callers catch for an invalid pattern
public class PatternSyntaxException extends IllegalArgumentException {

  private final String error; // the nature of the error
  private final String input; // the partial input at the point of error.

  public PatternSyntaxException(String error, String input) {
    super("error parsing regexp: " + error + ": `" + input + "`");
    this.error = error;
    this.input = input;
  }

  public PatternSyntaxException(String error) {
    super("error parsing regexp: " + error);
    this.error = error;
    this.input = "";
  }

  /**
   * Retrieves the error index.
   *
   * @return The approximate index in the pattern of the error, or {@code -1} if the index is not
   * known
   */
  public int getIndex() {
    return -1;
  }

  /**
   * Retrieves the description of the error.
   *
   * @return The description of the error
   */
  public String getDescription() {
    return error;
  }

  /**
   * Retrieves the erroneous regular-expression pattern.
   *
   * @return The erroneous pattern
   */
  public String getPattern() {
    return input;
  }
}
