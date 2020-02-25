package org.elasticsearch.xpack.ml.inference.search;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.fetch.FetchSubPhase;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.xpack.core.ml.inference.results.InferenceResults;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfig;
import org.elasticsearch.xpack.ml.inference.loadingservice.Model;
import org.elasticsearch.xpack.ml.inference.loadingservice.ModelLoadingService;

import java.io.IOException;
import java.util.Map;

/**
 * A very rough sketch of a fetch sub phase that performs inference on each search hit,
 * then augments the hit with the result.
 */
public class InferencePhase implements FetchSubPhase {
    private final String modelId;
    private final InferenceConfig config;
    private final SetOnce<ModelLoadingService> modelLoadingService;

    public InferencePhase(String modelId,
                          InferenceConfig config,
                          SetOnce<ModelLoadingService> modelLoadingService) {
        this.modelId = modelId;
        this.config = config;
        this.modelLoadingService = modelLoadingService;
    }

    @Override
    public void hitsExecute(SearchContext searchContext, SearchHit[] hits) throws IOException {
        SetOnce<Model> model = new SetOnce<>();
        modelLoadingService.get().getModel(modelId, ActionListener.wrap(
            model::set,
            m -> {throw new RuntimeException();}));

        for (SearchHit hit : hits) {
            // TODO: get fields through context.lookup() (or from the search hit?)
            Map<String, Object> document = Map.of();

            SetOnce<InferenceResults> result = new SetOnce<>();
            model.get().infer(document, config, ActionListener.wrap(
                result::set,
                m -> {throw new RuntimeException();}));

            // TODO: add inference result to hit.
        }
    }
}
