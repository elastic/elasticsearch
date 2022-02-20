/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.painless;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.painless.api.Augmentation;

import java.util.HashMap;
import java.util.Map;

/**
 * Settings to use when compiling a script.
 */
public final class CompilerSettings {
    /**
     * Are regexes enabled? If {@code true}, regexes are enabled and unlimited by the limit factor.  If {@code false}, they are completely
     * disabled. If {@code use-limit}, the default, regexes are enabled but limited in complexity according to the
     * {@code script.painless.regex.limit-factor} setting.
     */
    public static final Setting<RegexEnabled> REGEX_ENABLED = new Setting<>(
        "script.painless.regex.enabled",
        RegexEnabled.LIMITED.value,
        RegexEnabled::parse,
        Property.NodeScope
    );

    /**
     * How complex can a regex be?  This is the number of characters that can be considered expressed as a multiple of string length.
     */
    public static final Setting<Integer> REGEX_LIMIT_FACTOR = Setting.intSetting(
        "script.painless.regex.limit-factor",
        6,
        1,
        Property.NodeScope
    );

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
    private int testInject0 = 2;
    private int testInject1 = 4;
    private int testInject2 = 6;

    /**
     * Are regexes enabled? Defaults to using the factor setting.
     */
    private RegexEnabled regexesEnabled = RegexEnabled.LIMITED;

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
     * Are regexes enabled or limited?
     */
    public void setRegexesEnabled(RegexEnabled regexesEnabled) {
        this.regexesEnabled = regexesEnabled;
    }

    /**
     * What is the limitation on regex complexity?  How many multiples of input length can a regular expression consider?
     */
    public void setRegexLimitFactor(int regexLimitFactor) {
        this.regexLimitFactor = regexLimitFactor;
    }

    /**
     * What is the limit factor for regexes?
     */
    public int getRegexLimitFactor() {
        return regexLimitFactor;
    }

    /**
     * Get compiler settings as a map.  This is used to inject compiler settings into augmented methods with the {@code @inject_constant}
     * annotation.
     */
    public Map<String, Object> asMap() {
        int regexLimitFactorToApply = this.regexLimitFactor;
        if (regexesEnabled == RegexEnabled.TRUE) {
            regexLimitFactorToApply = Augmentation.UNLIMITED_PATTERN_FACTOR;
        } else if (regexesEnabled == RegexEnabled.FALSE) {
            regexLimitFactorToApply = Augmentation.DISABLED_PATTERN_FACTOR;
        }
        Map<String, Object> map = new HashMap<>();
        map.put("regex_limit_factor", regexLimitFactorToApply);

        // for testing only
        map.put("testInject0", testInject0);
        map.put("testInject1", testInject1);
        map.put("testInject2", testInject2);

        return map;
    }

    /**
     * Options for {@code script.painless.regex.enabled} setting.
     */
    public enum RegexEnabled {
        TRUE("true"),
        FALSE("false"),
        LIMITED("limited");

        final String value;

        RegexEnabled(String value) {
            this.value = value;
        }

        /**
         * Parse string value, necessary because `valueOf` would require strings to be upper case.
         */
        public static RegexEnabled parse(String value) {
            if (TRUE.value.equals(value)) {
                return TRUE;
            } else if (FALSE.value.equals(value)) {
                return FALSE;
            } else if (LIMITED.value.equals(value)) {
                return LIMITED;
            }
            throw new IllegalArgumentException(
                "invalid value [" + value + "] must be one of [" + TRUE.value + "," + FALSE.value + "," + LIMITED.value + "]"
            );
        }
    }
}
