/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 *
 * Forked from RE2/J 1.8, see package-info.java.
 *
 * Copyright (c) 2021 The Go Authors. All rights reserved.
 *
 * Use of this source code is governed by a BSD-style
 * license that can be found in the LICENSE file.
 */
package org.elasticsearch.xpack.esql.core.expression.predicate.regex.re2;

import java.nio.charset.Charset;

/**
 * Abstract the representations of input text supplied to Matcher.
 */
abstract class MatcherInput {

  enum Encoding {
    UTF_16,
    UTF_8,
  }

  /**
   * Return the MatcherInput for UTF_16 encoding.
   */
  static MatcherInput utf16(CharSequence charSequence) {
    return new Utf16MatcherInput(charSequence);
  }

  /**
   * Return the MatcherInput for UTF_8 encoding.
   */
  static MatcherInput utf8(byte[] bytes) {
    return new Utf8MatcherInput(bytes);
  }

  /**
   * Return the MatcherInput for UTF_8 encoding.
   */
  static MatcherInput utf8(String input) {
    return new Utf8MatcherInput(input.getBytes(Charset.forName("UTF-8")));
  }

  abstract Encoding getEncoding();

  abstract CharSequence asCharSequence();

  abstract byte[] asBytes();

  abstract int length();

  static class Utf8MatcherInput extends MatcherInput {
    byte[] bytes;

    // ES: no public on a constructor of a private class, as checkstyle wants
    Utf8MatcherInput(byte[] bytes) {
      this.bytes = bytes;
    }

    @Override
    public Encoding getEncoding() {
      return Encoding.UTF_8;
    }

    @Override
    public CharSequence asCharSequence() {
      return new String(bytes, Charset.forName("UTF-8"));
    }

    @Override
    public byte[] asBytes() {
      return bytes;
    }

    @Override
    public int length() {
      return bytes.length;
    }
  }

  static class Utf16MatcherInput extends MatcherInput {
    CharSequence charSequence;

    // ES: no public on a constructor of a private class, as checkstyle wants
    Utf16MatcherInput(CharSequence charSequence) {
      this.charSequence = charSequence;
    }

    @Override
    public Encoding getEncoding() {
      return Encoding.UTF_16;
    }

    @Override
    public CharSequence asCharSequence() {
      return charSequence;
    }

    @Override
    public byte[] asBytes() {
      return charSequence.toString().getBytes(Charset.forName("UTF-16"));
    }

    @Override
    public int length() {
      return charSequence.length();
    }
  }
}
