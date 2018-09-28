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

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.common.geo.GeoDistance;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.GeoUtils;
import org.elasticsearch.common.joda.JodaDateMathParser;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.script.JodaCompatibleZonedDateTime;
import org.elasticsearch.script.ScoreScript;
import org.elasticsearch.search.lookup.LeafSearchLookup;
import org.elasticsearch.search.lookup.SearchLookup;

import java.time.ZoneId;
import java.util.HashMap;
import java.util.Map;
import java.util.function.DoubleSupplier;
import java.util.function.Function;

/**
 * ScoringScriptImpl can be used as {@link ScoreScript}
 * to run a previously compiled Painless script.
 */
public final class ScoreScriptImpl extends ScoreScript {

    /**
     * The Painless script that can be run.
     */
    private final GenericElasticsearchScript script;

    /**
     * A map that can be used to access input parameters at run-time.
     */
    private final Map<String, Object> variables;

    /**
     * Looks up the {@code _score} from {@code #scorer} if {@code _score} is used, otherwise returns {@code 0.0}.
     */
    private final DoubleSupplier scoreLookup;

    /**
     * Looks up the {@code ctx} from the {@link #variables} if {@code ctx} is used, otherwise return {@code null}.
     */
    private final Function<Map<String, Object>, Map<?, ?>> ctxLookup;

    /**
     * Creates a ScriptImpl for the a previously compiled Painless script.
     * @param script The previously compiled Painless script.
     * @param vars The initial variables to run the script with.
     * @param lookup The lookup to allow search fields to be available if this is run as a search script.
     */
    ScoreScriptImpl(GenericElasticsearchScript script, Map<String, Object> vars, SearchLookup lookup, LeafReaderContext leafContext) {
        super(null, lookup, leafContext);
        this.script = script;
        this.variables = new HashMap<>();

        if (vars != null) {
            variables.putAll(vars);
        }
        LeafSearchLookup leafLookup = getLeafLookup();
        if (leafLookup != null) {
            variables.putAll(leafLookup.asMap());
        }

        scoreLookup = script.needs_score() ? this::getScore : () -> 0.0;
        ctxLookup = script.needsCtx() ? variables -> (Map<?, ?>) variables.get("ctx") : variables -> null;
    }

    @Override
    public Map<String, Object> getParams() {
        return variables;
    }

    public void setNextVar(final String name, final Object value) {
        variables.put(name, value);
    }

    @Override
    public double execute() {
        return ((Number) script.execute(variables, scoreLookup.getAsDouble(), getDoc(), null, ctxLookup.apply(variables))).doubleValue();
    }

    /****** STATIC FUNCTIONS that can be used by users for score calculations **/

    public static double rational(double value, double k) {
        return value/ (k + value);
    }

    /**
     * Calculate a sigmoid of <code>value</code>
     * with scaling parameters <code>k</code> and <code>a</code>
     */
    public static double sigmoid(double value, double k, double a){
        return Math.pow(value,a) / (Math.pow(k,a) + Math.pow(value,a));
    }


    // **** Decay functions on geo field
    public static final class DecayGeoLinear {
        // cached variables calculated once per script execution
        double originLat;
        double originLon;
        double offset;
        double scaling;

        public DecayGeoLinear(String originStr, String scaleStr, String offsetStr, double decay) {
            GeoPoint origin = GeoUtils.parseGeoPoint(originStr, false);
            double scale = DistanceUnit.DEFAULT.parse(scaleStr, DistanceUnit.DEFAULT);
            this.originLat = origin.lat();
            this.originLon = origin.lon();
            this.offset = DistanceUnit.DEFAULT.parse(offsetStr, DistanceUnit.DEFAULT);
            this.scaling = scale / (1.0 - decay);
        }

        public double decayGeoLinear(GeoPoint docValue) {
            double distance = GeoDistance.ARC.calculate(originLat, originLon, docValue.lat(), docValue.lon(), DistanceUnit.METERS);
            distance = Math.max(0.0d, distance - offset);
            return Math.max(0.0, (scaling - distance) / scaling);
        }
    }

    public static final class DecayGeoExp {
        double originLat;
        double originLon;
        double offset;
        double scaling;

        public DecayGeoExp(String originStr, String scaleStr, String offsetStr, double decay) {
            GeoPoint origin = GeoUtils.parseGeoPoint(originStr, false);
            double scale = DistanceUnit.DEFAULT.parse(scaleStr, DistanceUnit.DEFAULT);
            this.originLat = origin.lat();
            this.originLon = origin.lon();
            this.offset = DistanceUnit.DEFAULT.parse(offsetStr, DistanceUnit.DEFAULT);
            this.scaling = Math.log(decay) / scale;
        }

        public double decayGeoExp(GeoPoint docValue) {
            double distance = GeoDistance.ARC.calculate(originLat, originLon, docValue.lat(), docValue.lon(), DistanceUnit.METERS);
            distance = Math.max(0.0d, distance - offset);
            return Math.exp(scaling * distance);
        }
    }

    public static final class DecayGeoGauss {
        double originLat;
        double originLon;
        double offset;
        double scaling;

        public DecayGeoGauss(String originStr, String scaleStr, String offsetStr, double decay) {
            GeoPoint origin = GeoUtils.parseGeoPoint(originStr, false);
            double scale = DistanceUnit.DEFAULT.parse(scaleStr, DistanceUnit.DEFAULT);
            this.originLat = origin.lat();
            this.originLon = origin.lon();
            this.offset = DistanceUnit.DEFAULT.parse(offsetStr, DistanceUnit.DEFAULT);
            this.scaling =  0.5 * Math.pow(scale, 2.0) / Math.log(decay);;
        }

        public double decayGeoGauss(GeoPoint docValue) {
            double distance = GeoDistance.ARC.calculate(originLat, originLon, docValue.lat(), docValue.lon(), DistanceUnit.METERS);
            distance = Math.max(0.0d, distance - offset);
            return Math.exp(0.5 * Math.pow(distance, 2.0) / scaling);
        }
    }

    // **** Decay functions on numeric field

    public static final class DecayNumericLinear {
        double origin;
        double offset;
        double scaling;

        public DecayNumericLinear(String originStr, String scaleStr, String offsetStr, double decay) {
            double scale = Double.parseDouble(scaleStr);
            this.origin = Double.parseDouble(originStr);
            this.offset = Double.parseDouble(offsetStr);
            this.scaling = scale / (1.0 - decay);
        }

        public double decayNumericLinear(double docValue) {
            double distance = Math.max(0.0d, Math.abs(docValue - origin) - offset);
            return Math.max(0.0, (scaling - distance) / scaling);
        }
    }

    public static final class DecayNumericExp {
        double origin;
        double offset;
        double scaling;

        public DecayNumericExp(String originStr, String scaleStr, String offsetStr, double decay) {
            double scale = Double.parseDouble(scaleStr);
            this.origin = Double.parseDouble(originStr);
            this.offset = Double.parseDouble(offsetStr);
            this.scaling = Math.log(decay) / scale;
        }

        public double decayNumericExp(double docValue) {
            double distance = Math.max(0.0d, Math.abs(docValue - origin) - offset);
            return Math.exp(scaling * distance);
        }
    }

    public static final class DecayNumericGauss {
        double origin;
        double offset;
        double scaling;

        public DecayNumericGauss(String originStr, String scaleStr, String offsetStr, double decay) {
            double scale = Double.parseDouble(scaleStr);
            this.origin = Double.parseDouble(originStr);
            this.offset = Double.parseDouble(offsetStr);
            this.scaling = 0.5 * Math.pow(scale, 2.0) / Math.log(decay);
        }

        public double decayNumericGauss(double docValue) {
            double distance = Math.max(0.0d, Math.abs(docValue - origin) - offset);
            return Math.exp(0.5 * Math.pow(distance, 2.0) / scaling);
        }
    }

    // **** Decay functions on date field

    /**
     * Limitations: since script functions don't have access to DateFieldMapper,
     * decay functions on dates are limited to dates in the default format and default time zone,
     * Also, using calculations with <code>now</code> are not allowed.
     *
     */
    private static final ZoneId defaultZoneId = ZoneId.of("UTC");
    private static final JodaDateMathParser dateParser =  new JodaDateMathParser(DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER);

    public static final class DecayDateLinear {
        long origin;
        long offset;
        double scaling;

        public DecayDateLinear(String originStr, String scaleStr, String offsetStr, double decay) {
            this.origin = dateParser.parse(originStr, null, false, defaultZoneId);
            long scale = TimeValue.parseTimeValue(scaleStr, TimeValue.timeValueHours(24), getClass().getSimpleName() + ".scale")
                .getMillis();
            this.offset = TimeValue.parseTimeValue(offsetStr, TimeValue.timeValueHours(24), getClass().getSimpleName() + ".offset")
                .getMillis();
            this.scaling = scale / (1.0 - decay);
        }

        public double decayDateLinear(JodaCompatibleZonedDateTime docValueDate) {
            long docValue = docValueDate.toInstant().toEpochMilli();
            // as java.lang.Math#abs(long) is a forbidden API, have to use this comparison instead
            long diff = (docValue >= origin) ? (docValue - origin) : (origin - docValue);
            long distance = Math.max(0, diff - offset);
            return Math.max(0.0, (scaling - distance) / scaling);
        }
    }

    public static final class DecayDateExp {
        long origin;
        long offset;
        double scaling;

        public DecayDateExp(String originStr, String scaleStr, String offsetStr, double decay) {
            this.origin = dateParser.parse(originStr, null, false, defaultZoneId);
            long scale = TimeValue.parseTimeValue(scaleStr, TimeValue.timeValueHours(24), getClass().getSimpleName() + ".scale")
                .getMillis();
            this.offset = TimeValue.parseTimeValue(offsetStr, TimeValue.timeValueHours(24), getClass().getSimpleName() + ".offset")
                .getMillis();
            this.scaling = Math.log(decay) / scale;
        }

        public double decayDateExp(JodaCompatibleZonedDateTime docValueDate) {
            long docValue = docValueDate.toInstant().toEpochMilli();
            long diff = (docValue >= origin) ? (docValue - origin) : (origin - docValue);
            long distance = Math.max(0, diff - offset);
            return Math.exp(scaling * distance);
        }
    }


    public static final class DecayDateGauss {
        long origin;
        long offset;
        double scaling;

        public DecayDateGauss(String originStr, String scaleStr, String offsetStr, double decay) {
            this.origin = dateParser.parse(originStr, null, false, defaultZoneId);
            long scale = TimeValue.parseTimeValue(scaleStr, TimeValue.timeValueHours(24), getClass().getSimpleName() + ".scale")
                .getMillis();
            this.offset = TimeValue.parseTimeValue(offsetStr, TimeValue.timeValueHours(24), getClass().getSimpleName() + ".offset")
                .getMillis();
            this.scaling = 0.5 * Math.pow(scale, 2.0) / Math.log(decay);
        }

        public double decayDateGauss(JodaCompatibleZonedDateTime docValueDate) {
            long docValue = docValueDate.toInstant().toEpochMilli();
            long diff = (docValue >= origin) ? (docValue - origin) : (origin - docValue);
            long distance = Math.max(0, diff - offset);
            return Math.exp(0.5 * Math.pow(distance, 2.0) / scaling);
        }
    }
}
