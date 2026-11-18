/*
 * Copyright (c) 2020 The Go Authors. All rights reserved.
 *
 * Use of this source code is governed by a BSD-style
 * license that can be found in the LICENSE file.
 */
package com.google.re2j;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * A compiled representation of an RE2 regular expression, mimicking the
 * {@code java.util.regex.Pattern} API.
 *
 * <p>
 * The matching functions take {@code String} arguments instead of the more general Java
 * {@code CharSequence} since the latter doesn't provide UTF-16 decoding.
 *
 * <p>
 * See the <a href='package.html'>package-level documentation</a> for an overview of how to use this
 * API.
 * </p>
 *
 * @author rsc@google.com (Russ Cox)
 */
public final class Pattern implements Serializable {
  /** Flag: case insensitive matching. */
  public static final int CASE_INSENSITIVE = 1;

  /** Flag: dot ({@code .}) matches all characters, including newline. */
  public static final int DOTALL = 2;

  /**
   * Flag: multiline matching: {@code ^} and {@code $} match at beginning and end of line, not just
   * beginning and end of input.
   */
  public static final int MULTILINE = 4;

  /**
   * Flag: Unicode groups (e.g. {@code \p\ Greek\} ) will be syntax errors.
   */
  public static final int DISABLE_UNICODE_GROUPS = 8;

  /**
   * Flag: matches longest possible string.
   */
  public static final int LONGEST_MATCH = 16;

  // The pattern string at construction time.
  private final String pattern;

  // The flags at construction time.
  private final int flags;

  // The compiled RE2 regexp.
  private transient final RE2 re2;

  // This is visible for testing.
  Pattern(String pattern, int flags, RE2 re2) {
    if (pattern == null) {
      throw new NullPointerException("pattern is null");
    }
    if (re2 == null) {
      throw new NullPointerException("re2 is null");
    }
    this.pattern = pattern;
    this.flags = flags;
    this.re2 = re2;
  }

  /**
   * Releases memory used by internal caches associated with this pattern. Does not change the
   * observable behaviour. Useful for tests that detect memory leaks via allocation tracking.
   */
  public void reset() {
    re2.reset();
  }

  /**
   * Returns the flags used in the constructor.
   */
  public int flags() {
    return flags;
  }

  /**
   * Returns the pattern used in the constructor.
   */
  public String pattern() {
    return pattern;
  }

  RE2 re2() {
    return re2;
  }

  /**
   * Creates and returns a new {@code Pattern} corresponding to compiling {@code regex} with the
   * default flags (0).
   *
   * @param regex the regular expression
   * @throws PatternSyntaxException if the pattern is malformed
   */
  public static Pattern compile(String regex) {
    return compile(regex, regex, 0);
  }

  /**
   * Creates and returns a new {@code Pattern} corresponding to compiling {@code regex} with the
   * given {@code flags}.
   *
   * @param regex the regular expression
   * @param flags bitwise OR of the flag constants {@code CASE_INSENSITIVE}, {@code DOTALL}, and
   * {@code MULTILINE}
   * @throws PatternSyntaxException if the regular expression is malformed
   * @throws IllegalArgumentException if an unknown flag is given
   */
  public static Pattern compile(String regex, int flags) {
    String flregex = regex;
    if ((flags & CASE_INSENSITIVE) != 0) {
      flregex = "(?i)" + flregex;
    }
    if ((flags & DOTALL) != 0) {
      flregex = "(?s)" + flregex;
    }
    if ((flags & MULTILINE) != 0) {
      flregex = "(?m)" + flregex;
    }
    if ((flags & ~(MULTILINE | DOTALL | CASE_INSENSITIVE | DISABLE_UNICODE_GROUPS | LONGEST_MATCH))
        != 0) {
      throw new IllegalArgumentException(
          "Flags should only be a combination "
              + "of MULTILINE, DOTALL, CASE_INSENSITIVE, DISABLE_UNICODE_GROUPS, LONGEST_MATCH");
    }
    return compile(flregex, regex, flags);
  }

  /**
   * Helper: create new Pattern with given regex and flags. Flregex is the regex with flags applied.
   */
  private static Pattern compile(String flregex, String regex, int flags) {
    int re2Flags = RE2.PERL;
    if ((flags & DISABLE_UNICODE_GROUPS) != 0) {
      re2Flags &= ~RE2.UNICODE_GROUPS;
    }
    return new Pattern(
        regex, flags, RE2.compileImpl(flregex, re2Flags, (flags & LONGEST_MATCH) != 0));
  }

  /**
   * Matches a string against a regular expression.
   *
   * @param regex the regular expression
   * @param input the input
   * @return true if the regular expression matches the entire input
   * @throws PatternSyntaxException if the regular expression is malformed
   */
  public static boolean matches(String regex, CharSequence input) {
    return compile(regex).matcher(input).matches();
  }

  public static boolean matches(String regex, byte[] input) {
    return compile(regex).matcher(input).matches();
  }

  public boolean matches(String input) {
    return this.matcher(input).matches();
  }

  public boolean matches(byte[] input) {
    return this.matcher(input).matches();
  }

  /**
   * Creates a new {@code Matcher} matching the pattern against the input.
   *
   * @param input the input string
   */
  public Matcher matcher(CharSequence input) {
    return new Matcher(this, input);
  }

  public Matcher matcher(byte[] input) {
    return new Matcher(this, MatcherInput.utf8(input));
  }

  // This is visible for testing.
  Matcher matcher(MatcherInput input) {
    return new Matcher(this, input);
  }

  /**
   * Splits input around instances of the regular expression. It returns an array giving the strings
   * that occur before, between, and after instances of the regular expression. Empty strings that
   * would occur at the end of the array are omitted.
   *
   * @param input the input string to be split
   * @return the split strings
   */
  public String[] split(String input) {
    return split(input, 0);
  }

  /**
   * Splits input around instances of the regular expression. It returns an array giving the strings
   * that occur before, between, and after instances of the regular expression.
   *
   * <p>
   * If {@code limit <= 0}, there is no limit on the size of the returned array. If
   * {@code limit == 0}, empty strings that would occur at the end of the array are omitted. If
   * {@code limit > 0}, at most limit strings are returned. The final string contains the remainder
   * of the input, possibly including additional matches of the pattern.
   *
   * @param input the input string to be split
   * @param limit the limit
   * @return the split strings
   */
  public String[] split(String input, int limit) {
    return split(new Matcher(this, input), limit);
  }

  /** Helper: run split on m's input. */
  private String[] split(Matcher m, int limit) {
    List<String> result = new ArrayList<String>();
    int emptiesSkipped = 0;
    int last = 0;

    while (m.find()) {
      if (last == 0 && m.end() == 0) {
        // Zero-width match at the beginning, skip (JDK8+ behavior).
        last = m.end();
        continue;
      }

      if (limit > 0 && result.size() == limit - 1) {
        // no more room for matches
        break;
      }

      if (last == m.start()) {
        if (limit == 0) {
          // Empty match, may or may not be trailing.
          emptiesSkipped++;
          last = m.end();
          continue;
        }
      } else {
        // If emptiesSkipped > 0 then limit == 0 and we have non-trailing empty matches to add before
        // this non-empty match.
        while (emptiesSkipped > 0) {
          result.add("");
          emptiesSkipped--;
        }
      }

      result.add(m.substring(last, m.start()));
      last = m.end();
    }

    if (limit == 0 && last != m.inputLength()) {
      // Unlimited match, no more delimiters but we have a non-empty input at the end. Catch up any skipped empty
      // matches, then emit the final match.
      while (emptiesSkipped > 0) {
        result.add("");
        emptiesSkipped--;
      }

      result.add(m.substring(last, m.inputLength()));
    }

    if (limit != 0 || result.isEmpty()) {
      result.add(m.substring(last, m.inputLength()));
    }

    return result.toArray(new String[0]);
  }

  /**
   * Returns a literal pattern string for the specified string.
   *
   * <p>
   * This method produces a string that can be used to create a <code>Pattern</code> that would
   * match the string <code>s</code> as if it were a literal pattern.
   * </p>
   * Metacharacters or escape sequences in the input sequence will be given no special meaning.
   *
   * @param s The string to be literalized
   * @return A literal string replacement
   */
  public static String quote(String s) {
    return RE2.quoteMeta(s);
  }

  @Override
  public String toString() {
    return pattern;
  }

  /**
   * Returns the program size of this pattern.
   *
   * <p>
   * Similar to the C++ implementation, the program size is a very approximate measure of a regexp's
   * "cost". Larger numbers are more expensive than smaller numbers.
   * </p>
   *
   * @return the program size of this pattern
   */
  public int programSize() {
    return re2.numberOfInstructions();
  }

  /**
   * Returns the number of capturing groups in this matcher's pattern. Group zero denotes the entire
   * pattern and is excluded from this count.
   *
   * @return the number of capturing groups in this pattern
   */
  public int groupCount() {
    return re2.numberOfCapturingGroups();
  }

  /**
   * Return a map of the capturing groups in this matcher's pattern, where key is the name and value
   * is the index of the group in the pattern.
   */
  public Map<String, Integer> namedGroups() {
    return Collections.unmodifiableMap(re2.namedGroups);
  }

  Object readResolve() {
    // The deserialized version will be missing the RE2 instance, so we need to create a new,
    // compiled version.
    return Pattern.compile(pattern, flags);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    Pattern other = (Pattern) o;
    return flags == other.flags && pattern.equals(other.pattern);
  }

  @Override
  public int hashCode() {
    int result = pattern.hashCode();
    result = 31 * result + flags;
    return result;
  }

  private static final long serialVersionUID = 0;
}
