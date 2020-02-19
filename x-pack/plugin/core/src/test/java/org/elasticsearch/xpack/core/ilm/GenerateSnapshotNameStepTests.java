package org.elasticsearch.xpack.core.ilm;

import static org.elasticsearch.xpack.core.ilm.GenerateSnapshotNameStep.generateSnapshotName;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.startsWith;

public class GenerateSnapshotNameStepTests extends AbstractStepTestCase<GenerateSnapshotNameStep> {

    @Override
    protected GenerateSnapshotNameStep createRandomInstance() {
        return new GenerateSnapshotNameStep(randomStepKey(), randomStepKey());
    }

    @Override
    protected GenerateSnapshotNameStep mutateInstance(GenerateSnapshotNameStep instance) {
        Step.StepKey key = instance.getKey();
        Step.StepKey nextKey = instance.getNextStepKey();

        switch (between(0, 1)) {
            case 0:
                key = new Step.StepKey(key.getPhase(), key.getAction(), key.getName() + randomAlphaOfLength(5));
                break;
            case 1:
                nextKey = new Step.StepKey(key.getPhase(), key.getAction(), key.getName() + randomAlphaOfLength(5));
                break;
            default:
                throw new AssertionError("Illegal randomisation branch");
        }
        return new GenerateSnapshotNameStep(key, nextKey);
    }

    @Override
    protected GenerateSnapshotNameStep copyInstance(GenerateSnapshotNameStep instance) {
        return new GenerateSnapshotNameStep(instance.getKey(), instance.getNextStepKey());
    }

    public void testNameGeneration() {
        long time = 1552684146542L; // Fri Mar 15 2019 21:09:06 UTC
        assertThat(generateSnapshotName("name"), startsWith("name-"));
        assertThat(generateSnapshotName("name").length(), greaterThan("name-".length()));

        GenerateSnapshotNameStep.ResolverContext resolverContext = new GenerateSnapshotNameStep.ResolverContext(time);
        assertThat(generateSnapshotName("<name-{now}>", resolverContext), startsWith("name-2019.03.15-"));
        assertThat(generateSnapshotName("<name-{now}>", resolverContext).length(), greaterThan("name-2019.03.15-".length()));

        assertThat(generateSnapshotName("<name-{now/M}>", resolverContext), startsWith("name-2019.03.01-"));

        assertThat(generateSnapshotName("<name-{now/m{yyyy-MM-dd.HH:mm:ss}}>", resolverContext), startsWith("name-2019-03-15.21:09:00-"));
    }
}
