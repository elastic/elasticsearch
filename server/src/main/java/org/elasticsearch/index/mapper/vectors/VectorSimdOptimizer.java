package org.elasticsearch.index.mapper.vectors;

import jdk.incubator.vector.FloatVector;
import jdk.incubator.vector.VectorSpecies;
import jdk.incubator.vector.VectorOperators;  


public final class VectorSimdOptimizer {            

    
    private static final VectorSpecies<Float> SPECIES = FloatVector.SPECIES_PREFERRED;

    private VectorSimdOptimizer() {
        
    }

    
    public static float dotProductSIMD(float[] a, float[] b) {
        int length = a.length;
        int loopBound = SPECIES.loopBound(length);
        
        
        FloatVector acc = FloatVector.zero(SPECIES);

        int i = 0;
        
        for (; i < loopBound; i += SPECIES.length()) {
            FloatVector va = FloatVector.fromArray(SPECIES, a, i);
            FloatVector vb = FloatVector.fromArray(SPECIES, b, i);
            acc = va.fma(vb, acc); 
        }

        
       
        float totalSum = acc.reduceLanes(VectorOperators.ADD);  

        
        for (; i < length; i++) {
            totalSum += a[i] * b[i];
        }

        return totalSum;
    }
}