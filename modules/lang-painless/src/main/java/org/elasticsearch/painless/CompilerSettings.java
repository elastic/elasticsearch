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

package org.elasticsearch.painless;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.painless.api.Augmentation;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Settings to use when compiling a script.
 */
public final class CompilerSettings {
    /**
     * Are regexes enabled? If
     */
    public static final Setting<RegexEnabled> REGEX_ENABLED =
        new Setting<>("script.painless.regex.enabled", RegexEnabled.USE_FACTOR.value, RegexEnabled::parse, Property.NodeScope);

    /**
     * How complex can a regex be?  This is the number of characters that can be considered expressed as a multiple of string length.
     */
    public static final Setting<Integer> REGEX_LIMIT_FACTOR =
        Setting.intSetting("script.painless.regex.limit-factor", 6, 1, Property.NodeScope);

    /**
     * Constant to be used when specifying the maximum loop counter when compiling a script.
     */
    public static final String MAX_LOOP_COUNTER = "max_loop_counter";

    /**
     * Constant to be used for enabling additional internal compilation checks (slower).
     */
    public static final String PICKY = "picky";

    /**
     * Hack to set the initial "depth" for the {@link DefBootstrap.PIC} and {@link DefBootstrap.MIC}. Only used for testing: do not
     * overwrite.
     */
    public static final String INITIAL_CALL_SITE_DEPTH = "initialCallSiteDepth";

    /**
     * The maximum number of statements allowed to be run in a loop.
     * For now the number is set fairly high to accommodate users
     * doing large update queries.
     */
    private int maxLoopCounter = 1000000;

    /**
     * Whether to throw exception on ambiguity or other internal parsing issues. This option
     * makes things slower too, it is only for debugging.
     */
    private boolean picky = false;

    /**
     * For testing. Do not use.
     */
    private int initialCallSiteDepth = 0;

    /**
     * Are regexes enabled? Defaults to using the factor setting.
     */
    private RegexEnabled regexesEnabled = RegexEnabled.USE_FACTOR;


    /**
     * How complex can regexes be?  Expressed as a multiple of the input string.
     */
    private int regexLimitFactor = 0;

    /**
     * Returns the value for the cumulative total number of statements that can be made in all loops
     * in a script before an exception is thrown.  This attempts to prevent infinite loops.  Note if
     * the counter is set to 0, no loop counter will be written.
     */
    public int getMaxLoopCounter() {
        return maxLoopCounter;
    }

    /**
     * Set the cumulative total number of statements that can be made in all loops.
     * @see #getMaxLoopCounter
     */
    public void setMaxLoopCounter(int max) {
        this.maxLoopCounter = max;
    }

    /**
     * Returns true if the compiler should be picky. This means it runs slower and enables additional
     * runtime checks, throwing an exception if there are ambiguities in the grammar or other low level
     * parsing problems.
     */
    public boolean isPicky() {
      return picky;
    }

    /**
     * Set to true if compilation should be picky.
     * @see #isPicky
     */
    public void setPicky(boolean picky) {
      this.picky = picky;
    }

    /**
     * Returns initial call site depth. This means we pretend we've already seen N different types,
     * to better exercise fallback code in tests.
     */
    public int getInitialCallSiteDepth() {
        return initialCallSiteDepth;
    }

    /**
     * For testing megamorphic fallbacks. Do not use.
     * @see #getInitialCallSiteDepth()
     */
    public void setInitialCallSiteDepth(int depth) {
        this.initialCallSiteDepth = depth;
    }

    /**
     * Are regexes enabled?
     */
    public RegexEnabled areRegexesEnabled() {
        return regexesEnabled;
    }

    /**
     * Are regexes enabled? They are currently disabled by default because they break out of the loop counter and even fairly simple
     * <strong>looking</strong> regexes can cause stack overflows.
     */
    public void setRegexesEnabled(RegexEnabled regexesEnabled) {
        this.regexesEnabled = regexesEnabled;
    }

    public void setRegexLimitFactor(int regexLimitFactor) {
        this.regexLimitFactor = regexLimitFactor;
    }

    public int getRegexLimitFactor() {
        return regexLimitFactor;
    }

    public Map<String, Object> asMap() {
        int regexLimitFactor = this.regexLimitFactor;
        if (regexesEnabled == RegexEnabled.TRUE) {
            regexLimitFactor = Augmentation.UNLIMITED_PATTERN_FACTOR;
        } else if (regexesEnabled == RegexEnabled.FALSE) {
            regexLimitFactor = Augmentation.DISABLED_PATTERN_FACTOR;
        }
        Map<String, Object> map = new HashMap<>();
        map.put("foo", 246);
        map.put("regex_limit_factor", regexLimitFactor);
        return map;
    }

    public enum RegexEnabled {
        TRUE("true"),
        FALSE("false"),
        USE_FACTOR("use-factor");
        final String value;

        private RegexEnabled(String value) {
            this.value = value;
        }

        public static RegexEnabled parse(String value) {
            if (TRUE.value.equals(value)) {
                return TRUE;
            } else if (FALSE.value.equals(value)) {
                return FALSE;
            } else if (USE_FACTOR.value.equals(value)) {
                return USE_FACTOR;
            }
            throw new IllegalArgumentException(
                "invalid value [" + value + "] must be one of [" + TRUE.value + "," + FALSE.value + "," + USE_FACTOR.value + "]"
            );
        }
    }
}
