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

/**
 * Settings to use when compiling a script.
 */
public final class CompilerSettings {
    /**
     * Are regexes enabled? This is a node level setting because regexes break out of painless's lovely sandbox and can cause stack
     * overflows and we can't analyze the regex to be sure it won't.
     */
    public static final Setting<Boolean> REGEX_ENABLED = Setting.boolSetting("script.painless.regex.enabled", false, Property.NodeScope);

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
     * Are regexes enabled? They are currently disabled by default because they break out of the loop counter and even fairly simple
     * <strong>looking</strong> regexes can cause stack overflows.
     */
    private boolean regexesEnabled = false;

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
     * Are regexes enabled? They are currently disabled by default because they break out of the loop counter and even fairly simple
     * <strong>looking</strong> regexes can cause stack overflows.
     */
    public boolean areRegexesEnabled() {
        return regexesEnabled;
    }

    /**
     * Are regexes enabled? They are currently disabled by default because they break out of the loop counter and even fairly simple
     * <strong>looking</strong> regexes can cause stack overflows.
     */
    public void setRegexesEnabled(boolean regexesEnabled) {
        this.regexesEnabled = regexesEnabled;
    }
}
