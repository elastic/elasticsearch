/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.wildcard.mapper;


import java.util.Locale;

public class NGramApproximationSettings {
    private int maxExpand = 4;
    private int maxStatesTraced = 10000;
    private int maxDeterminizedStates = 20000;
    private int maxNgramsExtracted = 100;
    private int maxInspect = Integer.MAX_VALUE;
    private boolean caseSensitive = false;
    private Locale locale = Locale.ROOT;
    private boolean rejectUnaccelerated = false;

    public int getMaxExpand() {
        return maxExpand;
    }

    public void setMaxExpand(int maxExpand) {
        this.maxExpand = maxExpand;
    }

    public int getMaxStatesTraced() {
        return maxStatesTraced;
    }

    public void setMaxStatesTraced(int maxStatesTraced) {
        this.maxStatesTraced = maxStatesTraced;
    }

    public int getMaxDeterminizedStates() {
        return maxDeterminizedStates;
    }

    public void setMaxDeterminizedStates(int maxDeterminizedStates) {
        this.maxDeterminizedStates = maxDeterminizedStates;
    }

    public int getMaxNgramsExtracted() {
        return maxNgramsExtracted;
    }

    public void setMaxNgramsExtracted(int maxNgramsExtracted) {
        this.maxNgramsExtracted = maxNgramsExtracted;
    }

    public int getMaxInspect() {
        return maxInspect;
    }

    public void setMaxInspect(int maxInspect) {
        this.maxInspect = maxInspect;
    }

    public boolean getCaseSensitive() {
        return caseSensitive;
    }

    public void setCaseSensitive(boolean caseSensitive) {
        this.caseSensitive = caseSensitive;
    }

    public Locale getLocale() {
        return locale;
    }

    public void setLocale(Locale locale) {
        this.locale = locale;
    }

    public boolean getRejectUnaccelerated() {
        return rejectUnaccelerated;
    }

    public void setRejectUnaccelerated(boolean rejectUnaccelerated) {
        this.rejectUnaccelerated = rejectUnaccelerated;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (caseSensitive ? 1231 : 1237);
        result = prime * result + ((locale == null) ? 0 : locale.hashCode());
        result = prime * result + maxDeterminizedStates;
        result = prime * result + maxExpand;
        result = prime * result + maxInspect;
        result = prime * result + maxNgramsExtracted;
        result = prime * result + maxStatesTraced;
        result = prime * result + (rejectUnaccelerated ? 1231 : 1237);
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        NGramApproximationSettings other = (NGramApproximationSettings) obj;
        if (caseSensitive != other.caseSensitive)
            return false;
        if (locale == null) {
            if (other.locale != null)
                return false;
        } else if (!locale.equals(other.locale))
            return false;
        if (maxDeterminizedStates != other.maxDeterminizedStates)
            return false;
        if (maxExpand != other.maxExpand)
            return false;
        if (maxInspect != other.maxInspect)
            return false;
        if (maxNgramsExtracted != other.maxNgramsExtracted)
            return false;
        if (maxStatesTraced != other.maxStatesTraced)
            return false;
        if (rejectUnaccelerated != other.rejectUnaccelerated)
            return false;
        return true;
    }
}    

