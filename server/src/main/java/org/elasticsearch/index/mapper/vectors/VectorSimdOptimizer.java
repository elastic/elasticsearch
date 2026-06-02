package org.elasticsearch.index.mapper.vectors;

import java.lang.reflect.Method;

public class VectorSimdOptimizer {
    private static final Method SIMD_METHOD;

    static {
        Method method = null;
        try {
            Class<?> clazz = Class.forName("org.elasticsearch.index.mapper.vectors.VectorSimdOptimizerJava21");
            method = clazz.getMethod("dotProductSIMD", float[].class, float[].class);
        } catch (Throwable t) {
            method = null;
        }
        SIMD_METHOD = method;
    }

    public static float dotProduct(float[] a, float[] b) {
        if (SIMD_METHOD != null) {
            try {
                return (float) SIMD_METHOD.invoke(null, (Object) a, (Object) b);
            } catch (Throwable t) {
                // Fallback silencioso se falhar a invocação por segurança
            }
        }
        return dotProductScalar(a, b);
    }

    private static float dotProductScalar(float[] a, float[] b) {
        float res = 0f;
        for (int i = 0; i < a.length; i++) {
            res += a[i] * b[i];
        }
        return res;
    }
}
