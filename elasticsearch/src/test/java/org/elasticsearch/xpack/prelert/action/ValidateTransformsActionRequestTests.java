/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
public class ValidateTransformsActionRequestTests extends AbstractStreamableXContentTestCase<ValidateTransformsAction.Request> {

    @Override
    protected Request createTestInstance() {
        int size = randomInt(10);
        List<TransformConfig> transforms = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            TransformType transformType = randomFrom(TransformType.values());
            TransformConfig transform = new TransformConfig(transformType.prettyName());
            transforms.add(transform);
        }
        return new Request(transforms);
    }

    @Override
    protected Request createBlankInstance() {
        return new Request();
    }

    @Override
    protected Request parseInstance(XContentParser parser, ParseFieldMatcher matcher) {
        return Request.PARSER.apply(parser, () -> matcher);
    }

}
