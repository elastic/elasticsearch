/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.evaluator.command;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.operator.ColumnExtractOperator;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

public class IpLookupEvaluator implements ColumnExtractOperator.Evaluator {

    public static final String DEFAULT_GEOIP_DATABASE_MMDB = "GeoLite2-City.mmdb";

    private final String databaseFile;
    private final Map<String, DataType> geoLocationFieldTemplates;
    private final DataType inputType;

    public IpLookupEvaluator(String databaseFile, Map<String, DataType> geoLocationFieldTemplates, DataType inputType) {
        // todo - instead of keeping the databaseFile string, we should resolve the DB reader here and keep it for lookups
        if (databaseFile == null || databaseFile.isBlank()) {
            databaseFile = DEFAULT_GEOIP_DATABASE_MMDB;
        }
        this.databaseFile = databaseFile;
        this.geoLocationFieldTemplates = geoLocationFieldTemplates;
        this.inputType = inputType;
    }

    @Override
    public void computeRow(BytesRefBlock input, int row, Block.Builder[] target, BytesRef spare) {
        String ip = null;
        if (input.isNull(row) == false) {
            BytesRef bytes = input.getBytesRef(input.getFirstValueIndex(row), spare);
            if (inputType == DataType.IP) {
                ip = EsqlDataTypeConverter.ipToString(bytes);
            } else {
                ip = bytes.utf8ToString();
            }
        }

        Map<String, Object> geoData = lookupGeoData(ip);

        int i = 0;
        for (Map.Entry<String, DataType> entry : geoLocationFieldTemplates.entrySet()) {
            String relativeKey = entry.getKey();
            DataType dataType = entry.getValue();
            Object value = geoData.get(relativeKey);
            Block.Builder blockBuilder = target[i];

            if (value == null) {
                blockBuilder.appendNull();
            } else {
                switch (dataType) {
                    case KEYWORD:
                    case TEXT:
                        if (blockBuilder instanceof BytesRefBlock.Builder brbb) {
                            brbb.appendBytesRef(new BytesRef(value.toString()));
                        } else {
                            throw new EsqlIllegalArgumentException("Incorrect block builder for type [" + dataType + "]");
                        }
                        break;
                    case IP:
                        if (blockBuilder instanceof BytesRefBlock.Builder brbb) {
                            if (value instanceof BytesRef) {
                                brbb.appendBytesRef((BytesRef) value);
                            } else {
                                brbb.appendBytesRef(EsqlDataTypeConverter.stringToIP(value.toString()));
                            }
                        } else {
                            throw new EsqlIllegalArgumentException("Incorrect block builder for type [" + dataType + "]");
                        }
                        break;
                    case DOUBLE:
                        if (blockBuilder instanceof DoubleBlock.Builder dbb) {
                            dbb.appendDouble(((Number) value).doubleValue());
                        } else {
                            throw new EsqlIllegalArgumentException("Incorrect block builder for type [" + dataType + "]");
                        }
                        break;
                    case LONG:
                        if (blockBuilder instanceof LongBlock.Builder lbb) {
                            lbb.appendLong(((Number) value).longValue());
                        } else {
                            throw new EsqlIllegalArgumentException("Incorrect block builder for type [" + dataType + "]");
                        }
                        break;
                    case INTEGER:
                        if (blockBuilder instanceof IntBlock.Builder ibb) {
                            ibb.appendInt(((Number) value).intValue());
                        } else {
                            throw new EsqlIllegalArgumentException("Incorrect block builder for type [" + dataType + "]");
                        }
                        break;
                    case BOOLEAN:
                        if (blockBuilder instanceof BooleanBlock.Builder bbb) {
                            bbb.appendBoolean((Boolean) value);
                        } else {
                            throw new EsqlIllegalArgumentException("Incorrect block builder for type [" + dataType + "]");
                        }
                        break;
                    case GEO_POINT:
                        if (blockBuilder instanceof BytesRefBlock.Builder brbb) {
                            if (value instanceof GeoPoint gp) {
                                brbb.appendBytesRef(EsqlDataTypeConverter.stringToGeo(gp.toWKT()));
                            } else {
                                throw new EsqlIllegalArgumentException(
                                    "Unsupported value type ["
                                        + value.getClass().getName()
                                        + "] for an output field of type ["
                                        + dataType
                                        + "]"
                                );
                            }
                        } else {
                            throw new EsqlIllegalArgumentException("Incorrect block builder for type [" + dataType + "]");
                        }
                        break;
                    default:
                        throw new EsqlIllegalArgumentException(
                            "Unsupported DataType [" + dataType + "] for GeoIP output field [" + relativeKey + "]"
                        );
                }
            }
            i++;
        }
    }

    private static Map<String, Object> lookupGeoData(String ip) {
        // todo - what do we do if null or invalid ip?
        if (ip == null || ip.isBlank()) {
            return Collections.emptyMap();
        }
        LinkedHashMap<String, Object> geoData = new LinkedHashMap<>();
        switch (ip) {
            case "8.8.8.8":
                geoData.putLast("continent_name", "North America");
                geoData.putLast("country_iso_code", "US");
                geoData.putLast("region_name", "California");
                geoData.putLast("city_name", "Mountain View");
                geoData.putLast("location", new GeoPoint(37.751, -82.840));
                break;
            case "192.168.1.1":
                geoData.putLast("continent_name", "Europe");
                geoData.putLast("country_iso_code", "NL");
                geoData.putLast("region_name", "North Holland");
                geoData.putLast("city_name", "Amsterdam");
                geoData.putLast("location", new GeoPoint(52.3676, 4.9041));
                break;
            default:
                break;
        }
        return geoData;
    }
}
