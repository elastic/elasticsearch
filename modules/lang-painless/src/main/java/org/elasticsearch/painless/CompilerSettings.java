/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.painless;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.painless.api.Augmentation;

import java.util.HashMap;
import java.util.Map;

/**
 * Settings to use when compiling a script.
 */
public final class CompilerSettings {

    private static final Logger logger = LogManager.getLogger(CompilerSettings.class);

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

    /** Sentinel {@code -1b} for the allocation thresholds meaning that threshold is disabled. */
    public static final ByteSizeValue MAX_ALLOCATION_BYTES_DISABLED = ByteSizeValue.MINUS_ONE;

    /**
     * Per-context heuristic allocation limit, {@code script.painless.max_allocation_bytes.context.<context_name>.limit}.
     * The sentinel {@code -1b} (default) disables enforcement; any positive size enables it at that limit, and a script that
     * exceeds it is failed with an uncatchable error. {@link Property#NodeScope} only, since the limit changes generated
     * bytecode shape so a dynamic update would need a full compile-cache flush.
     * <p>
     * There is deliberately no upper bound. A sensible limit scales with heap, and heaps vary by orders of magnitude across
     * deployments, so any fixed ceiling is too low for someone. An unusably large limit simply never trips, which is the same
     * as leaving enforcement off — it cannot harm anything but the operator's own protection.
     * <p>
     * Tracking as a whole is enabled by this setting <i>or</i> {@link #WARN_ALLOCATION_BYTES}; see
     * {@link #isAllocationTrackingEnabled()}.
     */
    public static final Setting.AffixSetting<ByteSizeValue> MAX_ALLOCATION_BYTES = Setting.affixKeySetting(
        "script.painless.max_allocation_bytes.context.",
        "limit",
        key -> new Setting<>(key, MAX_ALLOCATION_BYTES_DISABLED.getStringRep(), s -> parseAllocationBytes(s, key), Property.NodeScope)
    );

    /**
     * Per-context allocation warning threshold,
     * {@code script.painless.max_allocation_bytes.context.<context_name>.warn_threshold}. The sentinel {@code -1b} (default)
     * disables warning; any positive size logs a {@code WARN} the first time a script execution's running total crosses it.
     * <p>
     * Crossing this threshold <b>never fails the script</b> — it only reports. It is deliberately independent of
     * {@link #MAX_ALLOCATION_BYTES} so an operator can observe which scripts are allocation-heavy before committing to
     * enforcement; setting it alone enables tracking with no enforcement at all. When enforcement <i>is</i> also on, the limit
     * acts as this threshold's effective ceiling and a higher value is clamped down to it; see
     * {@link #resolveWarnAllocationBytes}.
     */
    public static final Setting.AffixSetting<ByteSizeValue> WARN_ALLOCATION_BYTES = Setting.affixKeySetting(
        "script.painless.max_allocation_bytes.context.",
        "warn_threshold",
        key -> new Setting<>(key, MAX_ALLOCATION_BYTES_DISABLED.getStringRep(), s -> parseAllocationBytes(s, key), Property.NodeScope)
    );

    /**
     * Returns the warning threshold to actually use, clamping it down to the enforcement limit when it sits above one. A
     * warning threshold above the limit is dead configuration: the running total fails the script at the limit before it could
     * ever reach the higher threshold, so the warning could never be reported. Clamping keeps the node starting and leaves the
     * warning firing on the same allocation that trips the limit — which is useful rather than merely harmless, because the
     * warning message carries the script's name and source and the limit failure does not.
     * <p>
     * Either threshold alone always passes through unchanged, as does a warning threshold at or below the limit.
     *
     * @param contextName the script context the thresholds belong to, used to build the affix keys in the log message
     * @param limitBytes the enforcement limit in bytes, or {@code -1} when enforcement is off
     * @param warnBytes the configured warning threshold in bytes, or {@code -1} when warning is off
     * @param warnExplicitlySet whether the warning threshold was set in the node config rather than defaulted, which decides
     *                          only the log level: an operator whose own config was adjusted should investigate, whereas a
     *                          default we chose and then adjusted is not their problem
     * @return the effective warning threshold in bytes
     */
    public static long resolveWarnAllocationBytes(String contextName, long limitBytes, long warnBytes, boolean warnExplicitlySet) {
        if (limitBytes <= 0L || warnBytes <= 0L || warnBytes <= limitBytes) {
            return warnBytes;
        }

        String warnKey = WARN_ALLOCATION_BYTES.getConcreteSettingForNamespace(contextName).getKey();
        String limitKey = MAX_ALLOCATION_BYTES.getConcreteSettingForNamespace(contextName).getKey();
        String warnValue = ByteSizeValue.ofBytes(warnBytes).getStringRep();
        String limitValue = ByteSizeValue.ofBytes(limitBytes).getStringRep();

        if (warnExplicitlySet) {
            logger.warn(
                "[{}] is [{}], above [{}] [{}]; it could never be reported before the limit failed the script, so it has been "
                    + "clamped to [{}]. Lower it below the limit to silence this.",
                warnKey,
                warnValue,
                limitKey,
                limitValue,
                limitValue
            );
        } else {
            logger.info(
                "default [{}] of [{}] is above the configured [{}] [{}]; using [{}] instead",
                warnKey,
                warnValue,
                limitKey,
                limitValue,
                limitValue
            );
        }

        return limitBytes;
    }

    /** Accepts the {@code -1b} sentinel or any positive size; rejects {@code 0b} and (via {@link ByteSizeValue}) other negatives. */
    static ByteSizeValue parseAllocationBytes(String value, String key) {
        ByteSizeValue parsed = ByteSizeValue.parseBytesSizeValue(value, key);
        long bytes = parsed.getBytes();
        if (bytes == MAX_ALLOCATION_BYTES_DISABLED.getBytes()) {
            return parsed;
        }
        if (bytes < 1L) {
            throw new IllegalArgumentException(
                "failed to parse value ["
                    + value
                    + "] for setting ["
                    + key
                    + "], must be ["
                    + MAX_ALLOCATION_BYTES_DISABLED.getStringRep()
                    + "] (tracking disabled) or a positive size"
            );
        }
        return parsed;
    }

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

    /** Per-context enforcement limit in bytes from {@link #MAX_ALLOCATION_BYTES}; {@code -1} disables enforcement. */
    private long maxAllocationBytes = MAX_ALLOCATION_BYTES_DISABLED.getBytes();

    /** Per-context warning threshold in bytes from {@link #WARN_ALLOCATION_BYTES}; {@code -1} disables warning. */
    private long warnAllocationBytes = MAX_ALLOCATION_BYTES_DISABLED.getBytes();

    /**
     * Name of the script context being compiled. Baked into the generated allocation checks so a threshold breach can report
     * and be counted per context; unlike a script name this has bounded cardinality, so it is safe as a metric attribute.
     */
    private String scriptContextName = "unknown";

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
     * What is the effective limit factor for regexes?
     */
    public int getAppliedRegexLimitFactor() {
        return switch (regexesEnabled) {
            case TRUE -> Augmentation.UNLIMITED_PATTERN_FACTOR;
            case FALSE -> Augmentation.DISABLED_PATTERN_FACTOR;
            case LIMITED -> regexLimitFactor;
        };
    }

    /** The per-context allocation limit in bytes, or {@code -1} when tracking is disabled. */
    public long getMaxAllocationBytes() {
        return maxAllocationBytes;
    }

    /** @see #getMaxAllocationBytes */
    public void setMaxAllocationBytes(long maxAllocationBytes) {
        this.maxAllocationBytes = maxAllocationBytes;
    }

    /** The per-context allocation warning threshold in bytes, or {@code -1} when warning is disabled. */
    public long getWarnAllocationBytes() {
        return warnAllocationBytes;
    }

    /** @see #getWarnAllocationBytes */
    public void setWarnAllocationBytes(long warnAllocationBytes) {
        this.warnAllocationBytes = warnAllocationBytes;
    }

    /** @see #scriptContextName */
    public String getScriptContextName() {
        return scriptContextName;
    }

    /** @see #scriptContextName */
    public void setScriptContextName(String scriptContextName) {
        this.scriptContextName = scriptContextName;
    }

    /** Whether exceeding {@link #getMaxAllocationBytes} fails the script. */
    public boolean isAllocationLimitEnabled() {
        return maxAllocationBytes > 0L;
    }

    /** Whether crossing {@link #getWarnAllocationBytes} logs a warning. */
    public boolean isAllocationWarningEnabled() {
        return warnAllocationBytes > 0L;
    }

    /**
     * Whether allocation tracking is enabled at all, i.e. either threshold is positive. This is the gate for emitting any
     * tracking bytecode: warning alone is enough, since an operator may want to observe allocation without enforcing a limit.
     */
    public boolean isAllocationTrackingEnabled() {
        return isAllocationLimitEnabled() || isAllocationWarningEnabled();
    }

    /**
     * Get compiler settings as a map.  This is used to inject compiler settings into augmented methods with the {@code @inject_constant}
     * annotation.
     */
    public Map<String, Object> asMap() {
        Map<String, Object> map = new HashMap<>();
        map.put("regex_limit_factor", getAppliedRegexLimitFactor());

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
