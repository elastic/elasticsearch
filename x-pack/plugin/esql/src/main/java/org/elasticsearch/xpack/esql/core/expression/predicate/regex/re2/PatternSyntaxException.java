/*
 * Copyright (c) 2020 The Go Authors. All rights reserved.
 *
 * Use of this source code is governed by a BSD-style
 * license that can be found in the LICENSE file.
 */
package com.google.re2j;

/**
 * An exception thrown by the parser if the pattern was invalid.
 *
 * Following {@code java.util.regex.PatternSyntaxException}, this is an unchecked exception.
 */
public class PatternSyntaxException extends RuntimeException {

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
   * @return The approximate index in the pattern of the error, or <tt>-1</tt> if the index is not
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
