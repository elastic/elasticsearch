package org.elasticsearch.search.aggregations.bucket.geogrid;

import org.elasticsearch.common.geo.GeoHashUtils;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.GeoUtils;

public class GeoHashHandler implements GeoHashTypeProvider {
    @Override
    public int getDefaultPrecision() {
        return 5;
    }

    @Override
    public int parsePrecisionString(String precision) {
        return GeoUtils.parsePrecisionString(precision);
    }

    @Override
    public int validatePrecision(int precision) {
        return GeoUtils.checkPrecisionRange(precision);
    }

    @Override
    public long calculateHash(double longitude, double latitude, int precision) {
        return GeoHashUtils.longEncode(longitude, latitude, precision);
    }

    @Override
    public String hashAsString(long hash) {
        return GeoHashUtils.stringEncode(hash);
    }

    @Override
    public GeoPoint hashAsObject(long hash) {
        return GeoPoint.fromGeohash(hash);
    }
}
