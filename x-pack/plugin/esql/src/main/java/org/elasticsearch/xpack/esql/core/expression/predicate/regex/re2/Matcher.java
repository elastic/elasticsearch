/*
 * Copyright (c) 2020 The Go Authors. All rights reserved.
 *
 * Use of this source code is governed by a BSD-style
 * license that can be found in the LICENSE file.
 */
package com.google.re2j;

import com.google.re2j.MatcherInput.Encoding;
import java.io.UnsupportedEncodingException;
import java.util.Map;

/**
 * A stateful iterator that interprets a regex {@code Pattern} on a specific input. Its interface
 * mimics the JDK 1.4.2 {@code java.util.regex.Matcher}.
 *
 * <p>
 * Conceptually, a Matcher consists of four parts:
 * <ol>
 * <li>A compiled regular expression {@code Pattern}, set at construction and fixed for the lifetime
 * of the matcher.</li>
 *
 * <li>The remainder of the input string, set at construction or {@link #reset()} and advanced by
 * each match operation such as {@link #find}, {@link #matches} or {@link #lookingAt}.</li>
 *
 * <li>The current match information, accessible via {@link #start}, {@link #end}, and
 * {@link #group}, and updated by each match operation.</li>
 *
 * <li>The append position, used and advanced by {@link #appendReplacement} and {@link #appendTail}
 * if performing a search and replace from the input to an external {@code StringBuffer}.
 *
 * </ol>
 *
 * <p>
 * See the <a href="package.html">package-level documentation</a> for an overview of how to use this
 * API.
 * </p>
 *
 * @author rsc@google.com (Russ Cox)
 */
public final class Matcher {
  // The pattern being matched.
  private final Pattern pattern;

  // The group indexes, in [start, end) pairs.  Zeroth pair is overall match.
  private final int[] groups;

  private final Map<String, Integer> namedGroups;

  // The number of submatches (groups) in the pattern.
  private final int groupCount;

  // The number of instructions in the pattern.
  private final int numberOfInstructions;

  private MatcherInput matcherInput;

  // The input length in UTF16 codes.
  private int inputLength;

  // The append position: where the next append should start.
  private int appendPos;

  // Is there a current match?
  private boolean hasMatch;

  // Have we found the submatches (groups) of the current match?
  // group[0], group[1] are set regardless.
  private boolean hasGroups;

  // The anchor flag to use when repeating the match to find subgroups.
  private int anchorFlag;

  private Matcher(Pattern pattern) {
    if (pattern == null) {
      throw new NullPointerException("pattern is null");
    }
    this.pattern = pattern;
    RE2 re2 = pattern.re2();
    groupCount = re2.numberOfCapturingGroups();
    groups = new int[2 + 2 * groupCount];
    namedGroups = re2.namedGroups;
    numberOfInstructions = re2.numberOfInstructions();
  }

  /** Creates a new {@code Matcher} with the given pattern and input. */
  Matcher(Pattern pattern, CharSequence input) {
    this(pattern);
    reset(input);
  }

  Matcher(Pattern pattern, MatcherInput input) {
    this(pattern);
    reset(input);
  }

  /** Returns the {@code Pattern} associated with this {@code Matcher}. */
  public Pattern pattern() {
    return pattern;
  }

  /**
   * Resets the {@code Matcher}, rewinding input and discarding any match information.
   *
   * @return the {@code Matcher} itself, for chained method calls
   */
  public Matcher reset() {
    inputLength = matcherInput.length();
    appendPos = 0;
    hasMatch = false;
    hasGroups = false;
    return this;
  }

  /**
   * Resets the {@code Matcher} and changes the input.
   *
   * @param input the new input string
   * @return the {@code Matcher} itself, for chained method calls
   */
  public Matcher reset(CharSequence input) {
    return reset(MatcherInput.utf16(input));
  }

  /**
   * Resets the {@code Matcher} and changes the input.
   *
   * @param bytes utf8 bytes of the input string.
   * @return the {@code Matcher} itself, for chained method calls
   */
  public Matcher reset(byte[] bytes) {
    return reset(MatcherInput.utf8(bytes));
  }

  private Matcher reset(MatcherInput input) {
    if (input == null) {
      throw new NullPointerException("input is null");
    }
    matcherInput = input;
    reset();
    return this;
  }

  /**
   * Returns the start position of the most recent match.
   *
   * @throws IllegalStateException if there is no match
   */
  public int start() {
    return start(0);
  }

  /**
   * Returns the end position of the most recent match.
   *
   * @throws IllegalStateException if there is no match
   */
  public int end() {
    return end(0);
  }

  /**
   * Returns the start position of a subgroup of the most recent match.
   *
   * @param group the group index; 0 is the overall match
   * @throws IllegalStateException if there is no match
   * @throws IndexOutOfBoundsException if {@code group < 0} or {@code group > groupCount()}
   */
  public int start(int group) {
    loadGroup(group);
    return groups[2 * group];
  }

  /**
   * Returns the start of the named group of the most recent match, or -1 if the group was not
   * matched.
   *
   * @param group the group name
   * @throws IllegalArgumentException if no group with that name exists
   */
  public int start(String group) {
    Integer g = namedGroups.get(group);
    if (g == null) {
      throw new IllegalArgumentException("group '" + group + "' not found");
    }
    return start(g);
  }

  /**
   * Returns the end position of a subgroup of the most recent match.
   *
   * @param group the group index; 0 is the overall match
   * @throws IllegalStateException if there is no match
   * @throws IndexOutOfBoundsException if {@code group < 0} or {@code group > groupCount()}
   */
  public int end(int group) {
    loadGroup(group);
    return groups[2 * group + 1];
  }

  /**
   * Returns the end of the named group of the most recent match, or -1 if the group was not
   * matched.
   *
   * @param group the group name
   * @throws IllegalArgumentException if no group with that name exists
   */
  public int end(String group) {
    Integer g = namedGroups.get(group);
    if (g == null) {
      throw new IllegalArgumentException("group '" + group + "' not found");
    }
    return end(g);
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
    return numberOfInstructions;
  }

  /**
   * Returns the most recent match.
   *
   * @throws IllegalStateException if there is no match
   */
  public String group() {
    return group(0);
  }

  /**
   * Returns the subgroup of the most recent match.
   *
   * @throws IllegalStateException if there is no match
   * @throws IndexOutOfBoundsException if {@code group < 0} or {@code group > groupCount()}
   */
  public String group(int group) {
    int start = start(group);
    int end = end(group);
    if (start < 0 && end < 0) {
      // Means the subpattern didn't get matched at all.
      return null;
    }
    return substring(start, end);
  }

  /**
   * Returns the named group of the most recent match, or {@code null} if the group was not matched.
   *
   * @param group the group name
   * @throws IllegalArgumentException if no group with that name exists
   */
  public String group(String group) {
    Integer g = namedGroups.get(group);
    if (g == null) {
      throw new IllegalArgumentException("group '" + group + "' not found");
    }
    return group(g);
  }

  /**
   * Returns the number of subgroups in this pattern.
   *
   * @return the number of subgroups; the overall match (group 0) does not count
   */
  public int groupCount() {
    return groupCount;
  }

  /** Helper: finds subgroup information if needed for group. */
  private void loadGroup(int group) {
    if (group < 0 || group > groupCount) {
      throw new IndexOutOfBoundsException("Group index out of bounds: " + group);
    }
    if (!hasMatch) {
      throw new IllegalStateException("perhaps no match attempted");
    }
    if (group == 0 || hasGroups) {
      return;
    }

    // Include the character after the matched text (if there is one).
    // This is necessary in the case of inputSequence abc and pattern
    // (a)(b$)?(b)? . If we do pass in the trailing c,
    // the groups evaluate to new String[] {"ab", "a", null, "b" }
    // If we don't, they evaluate to new String[] {"ab", "a", "b", null}
    // We know it won't affect the total matched because the previous call
    // to match included the extra character, and it was not matched then.
    int end = groups[1] + 1;
    if (end > inputLength) {
      end = inputLength;
    }

    boolean ok =
        pattern.re2().match(matcherInput, groups[0], end, anchorFlag, groups, 1 + groupCount);
    // Must match - hasMatch says that the last call with these
    // parameters worked just fine.
    if (!ok) {
      throw new IllegalStateException("inconsistency in matching group data");
    }
    hasGroups = true;
  }

  /**
   * Matches the entire input against the pattern (anchored start and end). If there is a match,
   * {@code matches} sets the match state to describe it.
   *
   * @return true if the entire input matches the pattern
   */
  public boolean matches() {
    return genMatch(0, RE2.ANCHOR_BOTH);
  }

  /**
   * Matches the beginning of input against the pattern (anchored start). If there is a match,
   * {@code lookingAt} sets the match state to describe it.
   *
   * @return true if the beginning of the input matches the pattern
   */
  public boolean lookingAt() {
    return genMatch(0, RE2.ANCHOR_START);
  }

  /**
   * Matches the input against the pattern (unanchored). The search begins at the end of the last
   * match, or else the beginning of the input. If there is a match, {@code find} sets the match
   * state to describe it.
   *
   * @return true if it finds a match
   */
  public boolean find() {
    int start = 0;
    if (hasMatch) {
      start = groups[1];
      if (groups[0] == groups[1]) { // empty match - nudge forward
        start++;
      }
    }
    return genMatch(start, RE2.UNANCHORED);
  }

  /**
   * Matches the input against the pattern (unanchored), starting at a specified position. If there
   * is a match, {@code find} sets the match state to describe it.
   *
   * @param start the input position where the search begins
   * @return true if it finds a match
   * @throws IndexOutOfBoundsException if start is not a valid input position
   */
  public boolean find(int start) {
    if (start < 0 || start > inputLength) {
      throw new IndexOutOfBoundsException("start index out of bounds: " + start);
    }
    reset();
    return genMatch(start, 0);
  }

  /** Helper: does match starting at start, with RE2 anchor flag. */
  private boolean genMatch(int startByte, int anchor) {
    // TODO(rsc): Is matches/lookingAt supposed to reset the append or input positions?
    // From the JDK docs, looks like no.
    boolean ok = pattern.re2().match(matcherInput, startByte, inputLength, anchor, groups, 1);
    if (!ok) {
      return false;
    }
    hasMatch = true;
    hasGroups = false;
    anchorFlag = anchor;

    return true;
  }

  /** Helper: return substring for [start, end). */
  String substring(int start, int end) {
    // UTF_8 is matched in binary mode. So slice the bytes.
    if (matcherInput.getEncoding() == Encoding.UTF_8) {
      try {
        return new String(matcherInput.asBytes(), start, end - start, "UTF-8");
      } catch (UnsupportedEncodingException e) {
        throw new RuntimeException(e); // Not possible.
      }
    }

    // This is fast for both StringBuilder and String.
    return matcherInput.asCharSequence().subSequence(start, end).toString();
  }

  /** Helper for Pattern: return input length. */
  int inputLength() {
    return inputLength;
  }

  /**
   * Quotes '\' and '$' in {@code s}, so that the returned string could be used in
   * {@link #appendReplacement} as a literal replacement of {@code s}.
   *
   * @param s the string to be quoted
   * @return the quoted string
   */
  public static String quoteReplacement(String s) {
    if (s.indexOf('\\') < 0 && s.indexOf('$') < 0) {
      return s;
    }
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < s.length(); ++i) {
      char c = s.charAt(i);
      if (c == '\\' || c == '$') {
        sb.append('\\');
      }
      sb.append(c);
    }
    return sb.toString();
  }

  /**
   * Appends to {@code sb} two strings: the text from the append position up to the beginning of the
   * most recent match, and then the replacement with submatch groups substituted for references of
   * the form {@code $n}, where {@code n} is the group number in decimal. It advances the append
   * position to where the most recent match ended.
   *
   * <p>
   * To embed a literal {@code $}, use \$ (actually {@code "\\$"} with string escapes). The escape
   * is only necessary when {@code $} is followed by a digit, but it is always allowed. Only
   * {@code $} and {@code \} need escaping, but any character can be escaped.
   *
   * <p>
   * The group number {@code n} in {@code $n} is always at least one digit and expands to use more
   * digits as long as the resulting number is a valid group number for this pattern. To cut it off
   * earlier, escape the first digit that should not be used.
   *
   * @param sb the {@link StringBuffer} to append to
   * @param replacement the replacement string
   * @return the {@code Matcher} itself, for chained method calls
   * @throws IllegalStateException if there was no most recent match
   * @throws IndexOutOfBoundsException if replacement refers to an invalid group
   */
  public Matcher appendReplacement(StringBuffer sb, String replacement) {
    StringBuilder result = new StringBuilder();
    appendReplacement(result, replacement);
    sb.append(result);
    return this;
  }

  /**
   * Appends to {@code sb} two strings: the text from the append position up to the beginning of the
   * most recent match, and then the replacement with submatch groups substituted for references of
   * the form {@code $n}, where {@code n} is the group number in decimal. It advances the append
   * position to where the most recent match ended.
   *
   * <p>
   * To embed a literal {@code $}, use \$ (actually {@code "\\$"} with string escapes). The escape
   * is only necessary when {@code $} is followed by a digit, but it is always allowed. Only
   * {@code $} and {@code \} need escaping, but any character can be escaped.
   *
   * <p>
   * The group number {@code n} in {@code $n} is always at least one digit and expands to use more
   * digits as long as the resulting number is a valid group number for this pattern. To cut it off
   * earlier, escape the first digit that should not be used.
   *
   * @param sb the {@link StringBuilder} to append to
   * @param replacement the replacement string
   * @return the {@code Matcher} itself, for chained method calls
   * @throws IllegalStateException if there was no most recent match
   * @throws IndexOutOfBoundsException if replacement refers to an invalid group
   */
  public Matcher appendReplacement(StringBuilder sb, String replacement) {
    int s = start();
    int e = end();
    if (appendPos < s) {
      sb.append(substring(appendPos, s));
    }
    appendPos = e;
    appendReplacementInternal(sb, replacement);
    return this;
  }

  private void appendReplacementInternal(StringBuilder sb, String replacement) {
    int last = 0;
    int i = 0;
    int m = replacement.length();
    for (; i < m - 1; i++) {
      if (replacement.charAt(i) == '\\') {
        if (last < i) {
          sb.append(replacement.substring(last, i));
        }
        i++;
        last = i;
        continue;
      }
      if (replacement.charAt(i) == '$') {
        int c = replacement.charAt(i + 1);
        if ('0' <= c && c <= '9') {
          int n = c - '0';
          if (last < i) {
            sb.append(replacement.substring(last, i));
          }
          for (i += 2; i < m; i++) {
            c = replacement.charAt(i);
            if (c < '0' || c > '9' || n * 10 + c - '0' > groupCount) {
              break;
            }
            n = n * 10 + c - '0';
          }
          if (n > groupCount) {
            throw new IndexOutOfBoundsException("n > number of groups: " + n);
          }
          String group = group(n);
          if (group != null) {
            sb.append(group);
          }
          last = i;
          i--;
          continue;
        } else if (c == '{') {
          if (last < i) {
            sb.append(replacement.substring(last, i));
          }
          i++; // skip {
          int j = i + 1;
          while (j < replacement.length()
              && replacement.charAt(j) != '}'
              && replacement.charAt(j) != ' ') {
            j++;
          }
          if (j == replacement.length() || replacement.charAt(j) != '}') {
            throw new IllegalArgumentException("named capture group is missing trailing '}'");
          }
          String groupName = replacement.substring(i + 1, j);
          sb.append(group(groupName));
          last = j + 1;
        }
      }
    }
    if (last < m) {
      sb.append(replacement, last, m);
    }
  }

  /**
   * Appends to {@code sb} the substring of the input from the append position to the end of the
   * input.
   *
   * @param sb the {@link StringBuffer} to append to
   * @return the argument {@code sb}, for method chaining
   */
  public StringBuffer appendTail(StringBuffer sb) {
    sb.append(substring(appendPos, inputLength));
    return sb;
  }

  /**
   * Appends to {@code sb} the substring of the input from the append position to the end of the
   * input.
   *
   * @param sb the {@link StringBuilder} to append to
   * @return the argument {@code sb}, for method chaining
   */
  public StringBuilder appendTail(StringBuilder sb) {
    sb.append(substring(appendPos, inputLength));
    return sb;
  }

  /**
   * Returns the input with all matches replaced by {@code replacement}, interpreted as for
   * {@code appendReplacement}.
   *
   * @param replacement the replacement string
   * @return the input string with the matches replaced
   * @throws IndexOutOfBoundsException if replacement refers to an invalid group
   */
  public String replaceAll(String replacement) {
    return replace(replacement, true);
  }

  /**
   * Returns the input with the first match replaced by {@code replacement}, interpreted as for
   * {@code appendReplacement}.
   *
   * @param replacement the replacement string
   * @return the input string with the first match replaced
   * @throws IndexOutOfBoundsException if replacement refers to an invalid group
   */
  public String replaceFirst(String replacement) {
    return replace(replacement, false);
  }

  /** Helper: replaceAll/replaceFirst hybrid. */
  private String replace(String replacement, boolean all) {
    reset();
    StringBuffer sb = new StringBuffer();
    while (find()) {
      appendReplacement(sb, replacement);
      if (!all) {
        break;
      }
    }
    appendTail(sb);
    return sb.toString();
  }
}
