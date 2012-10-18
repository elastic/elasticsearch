/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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
package org.elasticsearch.search.facet.terms.comparator;

import org.elasticsearch.ElasticSearchIllegalArgumentException;

import java.text.Collator;
import java.text.ParseException;
import java.text.RuleBasedCollator;
import java.util.Locale;

/**
 * Abstract terms facet comparator, also serving as a factory
 * for TermsFacetComparator classes.
 * 
 * @author joerg
 */
public abstract class AbstractTermsFacetComparator implements TermsFacetComparator {

    private final String type;
    protected final boolean reverse;
    private Locale locale;

    public AbstractTermsFacetComparator(String type, boolean reverse) {
        this.type = type;
        this.reverse = reverse;
    }

    @Override
    public String getType() {
        return type;
    }
    
    @Override
    public boolean getReverse() {
        return reverse;
    }

    public AbstractTermsFacetComparator setLocale(Locale locale) {
        this.locale = locale;
        return this;
    }

    @Override
    public Locale getLocale() {
        return locale;
    }

    /**
     * Must override if own collator is used, otherwise always
     * the default collator of the given locale is used
     */
    @Override
    public int getDecomposition() {
        if (locale == null) {
            locale = Locale.getDefault();
        }
        return Collator.getInstance(locale).getDecomposition();
    }

    /**
     * Must override if own collator is used, otherwise always
     * the default collator of the given locale is used
     */
    @Override
    public int getStrength() {
        if (locale == null) {
            locale = Locale.getDefault();
        }
        return Collator.getInstance(locale).getStrength();
    }

    @Override
    public String getRules() {
        if (locale == null) {
            locale = Locale.getDefault();
        }
        return ((RuleBasedCollator) Collator.getInstance(locale)).getRules();
    }

    public static TermsFacetComparator getInstance(String type, boolean reverse, Locale locale, String rules, int decomp, int strength) {
        if (locale == null) {
            locale = Locale.getDefault();
        }
        if ("count".equals(type) || "reverse_count".equals(type) || "reverseCount".equals(type)) {
            if (type.startsWith("reverse")) {
                reverse = true; // compatibility mode
            }
            return new TermsFacetCountComparator("count", reverse);
        } else if ("term".equals(type) || "reverse_term".equals(type) || "reverseTerm".equals(type)) {
            if (type.startsWith("reverse")) {
                reverse = true; // compatibility mode
            }
            TermsFacetCollationComparator comparator = new TermsFacetCollationComparator("term", reverse);
            comparator.setLocale(locale);
            Collator collator = Collator.getInstance(locale);
            if (rules == null) {
                if (strength != -1) {
                    collator.setStrength(strength);
                }
                if (decomp != -1) {
                    collator.setDecomposition(decomp);
                }
                comparator.setCollator(collator);
            } else {
                try {
                    comparator.setRules(rules);
                    RuleBasedCollator rc = (RuleBasedCollator) collator;
                    comparator.setCollator(new RuleBasedCollator(rc.getRules() + rules));
                } catch (ParseException e) {
                    comparator.setCollator(Collator.getInstance(locale));
                }
            }
            return comparator;
        }
        throw new ElasticSearchIllegalArgumentException("No type argument match for terms facet comparator [" + type + "]");
    }
}
