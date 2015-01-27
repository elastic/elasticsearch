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
package org.elasticsearch.search.aggregations.support;

import org.elasticsearch.cache.recycler.PageCacheRecycler;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexGeoPointFieldData;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.fielddata.IndexOrdinalsFieldData;
import org.elasticsearch.index.fielddata.plain.ParentChildIndexFieldData;
import org.elasticsearch.search.aggregations.AggregationExecutionException;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;

/**
 *
 */
public class AggregationContext {

    private final SearchContext searchContext;

    public AggregationContext(SearchContext searchContext) {
        this.searchContext = searchContext;
    }

    public SearchContext searchContext() {
        return searchContext;
    }

    public PageCacheRecycler pageCacheRecycler() {
        return searchContext.pageCacheRecycler();
    }

    public BigArrays bigArrays() {
        return searchContext.bigArrays();
    }

    /** Get a value source given its configuration and the depth of the aggregator in the aggregation tree. */
    public <VS extends ValuesSource> VS valuesSource(ValuesSourceConfig<VS> config) throws IOException {
        assert config.valid() : "value source config is invalid - must have either a field context or a script or marked as unmapped";
        assert !config.unmapped : "value source should not be created for unmapped fields";

        if (config.fieldContext == null) {
            if (ValuesSource.Numeric.class.isAssignableFrom(config.valueSourceType)) {
                return (VS) numericScript(config);
            }
            if (ValuesSource.Bytes.class.isAssignableFrom(config.valueSourceType)) {
                return (VS) bytesScript(config);
            }
            throw new AggregationExecutionException("value source of type [" + config.valueSourceType.getSimpleName() + "] is not supported by scripts");
        }

        if (ValuesSource.Numeric.class.isAssignableFrom(config.valueSourceType)) {
            return (VS) numericField(config);
        }
        if (ValuesSource.GeoPoint.class.isAssignableFrom(config.valueSourceType)) {
            return (VS) geoPointField(config);
        }
        // falling back to bytes values
        return (VS) bytesField(config);
    }

    private ValuesSource.Numeric numericScript(ValuesSourceConfig<?> config) throws IOException {
        return new ValuesSource.Numeric.Script(config.script, config.scriptValueType);
    }

    private ValuesSource.Numeric numericField(ValuesSourceConfig<?> config) throws IOException {
        ValuesSource.Numeric dataSource = new ValuesSource.Numeric.FieldData((IndexNumericFieldData) config.fieldContext.indexFieldData());
        if (config.script != null) {
            dataSource = new ValuesSource.Numeric.WithScript(dataSource, config.script);
        }
        return dataSource;
    }

    private ValuesSource bytesField(ValuesSourceConfig<?> config) throws IOException {
        final IndexFieldData<?> indexFieldData = config.fieldContext.indexFieldData();
        ValuesSource dataSource;
        if (indexFieldData instanceof ParentChildIndexFieldData) {
            dataSource = new ValuesSource.Bytes.WithOrdinals.ParentChild((ParentChildIndexFieldData) indexFieldData);
        } else if (indexFieldData instanceof IndexOrdinalsFieldData) {
            dataSource = new ValuesSource.Bytes.WithOrdinals.FieldData((IndexOrdinalsFieldData) indexFieldData);
        } else {
            dataSource = new ValuesSource.Bytes.FieldData(indexFieldData);
        }
        if (config.script != null) {
            dataSource = new ValuesSource.WithScript(dataSource, config.script);
        }
        return dataSource;
    }

    private ValuesSource.Bytes bytesScript(ValuesSourceConfig<?> config) throws IOException {
        return new ValuesSource.Bytes.Script(config.script);
    }

    private ValuesSource.GeoPoint geoPointField(ValuesSourceConfig<?> config) throws IOException {
        return new ValuesSource.GeoPoint((IndexGeoPointFieldData) config.fieldContext.indexFieldData());
    }

}
