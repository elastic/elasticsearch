/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.spatial.projections;

import org.elasticsearch.test.ESTestCase;
import org.locationtech.proj4j.CRSFactory;
import org.locationtech.proj4j.CoordinateReferenceSystem;
import org.locationtech.proj4j.CoordinateTransform;
import org.locationtech.proj4j.CoordinateTransformFactory;
import org.locationtech.proj4j.ProjCoordinate;

import java.security.AccessController;
import java.security.PrivilegedAction;

public class GeospatialProjectionsTest extends ESTestCase {
    private boolean verbose = false;
    private static final CoordinateTransformFactory ctFactory = new CoordinateTransformFactory();
    private static CRSFactory crsFactory = AccessController.doPrivileged((PrivilegedAction<CRSFactory>) () -> new CRSFactory());

    public void test() {
        checkTransform("EPSG:4326", 3.8142776, 51.285914, "EPSG:23031", 556878.9016076007, 5682145.166264554, 0.1);
    }

    private void checkTransform(
        String srcCRS, double x1, double y1,
        String tgtCRS, double x2, double y2, double tolerance) {
        assertTrue(checkTransform(createCRS(srcCRS), x1, y1, createCRS(tgtCRS), x2, y2, tolerance));
    }

    public boolean checkTransform(
        CoordinateReferenceSystem srcCRS, double x1, double y1,
        CoordinateReferenceSystem tgtCRS, double x2, double y2,
        double tolerance) {
        return checkTransform(
            srcCRS, new ProjCoordinate(x1, y1),
            tgtCRS, new ProjCoordinate(x2, y2),
            tolerance);
    }

    public boolean checkTransform(
        CoordinateReferenceSystem srcCRS, ProjCoordinate p,
        CoordinateReferenceSystem tgtCRS, ProjCoordinate p2,
        double tolerance) {
        CoordinateTransform trans = ctFactory.createTransform(srcCRS, tgtCRS);
        ProjCoordinate pout = new ProjCoordinate();
        trans.transform(p, pout);

        double dx = Math.abs(pout.x - p2.x);
        double dy = Math.abs(pout.y - p2.y);
        double delta = Math.max(dx, dy);

        if (verbose) {
            System.out.println(crsDisplay(srcCRS) + " => " + crsDisplay(tgtCRS));
            System.out.println(
                p.toShortString()
                    + " -> "
                    + pout.toShortString()
                    + " (expected: " + p2.toShortString()
                    + " tol: " + tolerance + " diff: " + delta
                    + " )"
            );
        }

        boolean isInTol = delta <= tolerance;

        if (verbose && !isInTol) {
            System.out.println("FAIL");
            System.out.println("Src CRS: " + srcCRS.getParameterString());
            System.out.println("Tgt CRS: " + tgtCRS.getParameterString());
        }

        if (verbose) {
            System.out.println();
        }

        return isInTol;
    }

    private static String crsDisplay(CoordinateReferenceSystem crs) {
        return crs.getName()
            + "(" + crs.getProjection() + "/" + crs.getDatum().getCode() + ")";
    }

    private CoordinateReferenceSystem createCRS(String crsSpec) {
        CoordinateReferenceSystem crs = null;
        // test if name is a PROJ4 spec
        if (crsSpec.indexOf("+") >= 0 || crsSpec.indexOf("=") >= 0) {
            crs = crsFactory.createFromParameters("Anon", crsSpec);
        } else {
            crs = crsFactory.createFromName(crsSpec);
        }
        return crs;
    }
}
