# Change and anomaly detection for telemetry

This module finds the events that matter for root cause analysis in a numeric time series: **structural changes**
(a shift in level or trend), **distribution changes** (a shift in noise level / variance), and **point anomalies**
(isolated spikes and dips). It is designed to run on arbitrary real telemetry - series that may be near-constant,
smoothly drifting, heteroscedastic (quiet in places, noisy in others), sparse, or at very large magnitudes - and
to report only a small number of genuinely interesting events.

The single entry point is `EventDetector.detect(DoubleBucketValues)`, which returns a list of `ChangeType` events.
The concrete types are `StepChange`, `TrendChange`, `DistributionChange`, `Spike`, and `Dip`; every event carries
a change-point index (in source-bucket space) and a p-value.

## Design in one paragraph

The series is examined through three independent detectors and their events are merged. Two of them are the
*same* structural detector run on two channels: the raw values (catching level/trend changes) and a
dispersion channel - `log1p` of a robust local noise scale over non-overlapping windows - on which a change
in variance becomes an ordinary level change. The third detector finds point excursions from a local baseline.
Keeping the three concerns separate is what makes each one tractable and robust; the orchestration that ties
them together lives in `EventDetector`.

## Code layout

| File | Responsibility |
|------|----------------|
| `EventDetector` | Top-level orchestration: downsample, run the three detectors, de-duplicate, remap to source buckets. |
| `StructuralChangeDetector` | PELT segmentation of one channel into candidate boundaries. |
| `StructuralChangeClassifier` | Verifies/labels each candidate boundary as a step or trend by a BIC test. |
| `PulseDetector` | Spike/dip detection as point excursions from a local baseline. |
| `Stats` | Shared statistical primitives (robust scales, KDE, dispersion channel, regularizers). |
| `SeriesDownsampler` | Runtime safeguard that collapses an over-long series. |
| `LeastSquaresOnlineRegression` | Incremental weighted polynomial regression used by PELT. |
| `ChangeType` | The event types returned to the caller. |

## The structural channel (level and trend changes)

Candidate boundaries are found with PELT (Pruned Exact Linear Time), which minimises a penalised
segment cost over all segmentations in linear time. Each segment is fit with a constant-or-linear
(degree-1) model, and its cost is evaluated in O(1) from prefix-summed weighted moments of the
(down-weighted) series — a handful of array reads and a 2×2 solve rather than maintaining a regression
per candidate. Higher-order segments are unstable on the short windows PELT explores and tend to absorb
sharp trend onsets. The per-segment cost is the profiled-variance Gaussian cost
$\frac{L}{2}\cdot(\log(\text{RSS}/L) + 1)$ with a per-break penalty $\text{SEGMENT\_PENALTY\_BIAS} \cdot \beta$,
where $\beta = \text{betaMultiplier} \cdot (\text{degree}+1) \cdot \log(n)$ is the BIC complexity term.

The profiled (scale-free) cost is used deliberately rather than a cost against a fixed global variance:
the global noise estimate is unreliable on smooth signals (the MAD of first differences can collapse on
a slowly-varying series), which makes a fixed-variance cost over-segment them. The profiled cost found by
minimizing the log likelihood w.r.t. the free variance parameter depends only on the ratio $\text{RSS}/L$,
so it is immune to that, and a small RSS floor keeps the logarithm finite so a pristine segment is not
rewarded without bound.

Before PELT runs, the points are robustly down-weighted (`StructuralChangeDetector.localDeviationWeights`).
Each point is weighted by a redescending Tukey biweight of its residual from a rolling-median baseline, so
a short excursion loses influence on the fit while a sustained regime keeps full weight. Crucially the
residual is judged against a local robust scale (the MAD of residuals in a window of about a segment, capped
by the global composite scale), not a global one. On a heteroscedastic series this is essential: a spike
sitting on a low-variance regime is many *local* sigma and is suppressed, even though it is a fraction of
a sigma against a global scale dominated by some noisier regime elsewhere — and leaving it at full weight
would pull a nearby boundary off the true change.

PELT's candidates are then verified and classified (`StructuralChangeClassifier`). PELT already produces
a globally-optimal penalised segmentation, so its candidates are taken as the boundaries directly. Each
candidate is tested inside the window spanning to its nearest neighbouring candidates (kept at least
`minSegmentLength` away so a close neighbour cannot shrink the window below the verifier's minimum). The
test compares a no-change null against step/trend alternatives by BIC, maps the BIC gain to a p-value,
and keeps the most parsimonious alternative that (a) clears the significance threshold and (b) persists
when the weights immediately around the candidate are muted - if muting collapses the gain, the "change"
was driven by a few extreme points (a spike) and is rejected. The polynomial order is applied symmetrically
to the null and to each side of the split, so the alternative is the same model class split at the candidate
(strictly more flexible at equal degree).

## The distribution channel (variance changes)

A change in noise level is invisible to the mean channel (and the down-weighting there actively mutes the
excursions that carry it), so it is detected on a separate dispersion channel: one sample per non-overlapping
window, each being `log1p` of a robust scale of the window's first differences. On this channel a variance
change is just a level change, so it is found with the *same* `StructuralChangeDetector`.

Two choices matter here:

- **Non-overlapping windows.** Overlapping windows share most of their points, which would autocorrelate the
  channel and make the segmenter over-detect. Independent samples keep the mean-shift machinery valid.
- **The inter-quartile range of first differences**, not the median (too robust) and not the raw standard
  deviation (not robust enough). First-differencing cancels level and slope, so a mean step contributes a
  single large difference rather than inflating a window, and a steady ramp produces a flat channel. The IQR's
  25%-per-tail breakdown is the right middle ground: the median misses a window that is, say, 40% excursions
  then flat (it reads as zero), while the raw standard deviation lets a single spike's two large differences
  inflate a window. The dispersion detector also caps its no-change model lower than the value channel does,
  because its segments are much shorter (the channel is `1/window` the length): a more flexible null would
  absorb a genuine low-high-low variance bump.

## Point anomalies (spikes and dips)

Spikes and dips are point excursions from a local rolling-median baseline. Working off the local residual
rather than raw values versus a global centre means level structure is removed (a multi-level series cannot
mask an excursion on a low-level regime) and smooth curvature is tracked (a gently bending trend does not
leave the large residuals a piecewise-linear fit would, which would otherwise manufacture spurious
mid-segment spikes).

The pipeline is:

1. **Long list.** Flag points whose residual exceeds a threshold number of robust sigmas. The scale here is
   the larger of the global first-difference noise (which stays meaningful on smooth/monotone data, where the
   rolling-median residual is exactly zero for most points and a residual-only scale would collapse) and the
   composite scale of the residuals (which *inflates* once a frequent large-residual population appears, so a
   high-variance regime's ordinary fluctuations are not each flagged). For anomaly detection we want global
   extremes, so unlike the weighting this scale is global.
2. **Merge.** Adjacent same-sign candidates are merged into single excursions; a run spanning a full
   `minSegmentLength` is dropped (that is a regime, owned by the structural/dispersion channels).
3. **Limit.** The excursions are ranked by peak $z$ and capped at $\max(5, 2\% \cdot n)$, so a pathological
   series cannot flood the output.
4. **Gate.** A single Gaussian-KDE null is built from the series with **all** the retained excursions removed
   at once, and each excursion is kept only if its peak's Bonferroni-corrected tail probability under that
   null clears the threshold. Removing all the tested excursions together (rather than one at a time) is what
   lets several genuinely distinct spikes each be judged against the quiet remainder and all survive — scoring
   each against a null that still contained the others would let the largest spike and dip mask all the rest.
   A recurring population is still rejected, because its representatives are judged against their many
   remaining peers.

## Orchestration, de-duplication and indices

`EventDetector` runs the structural value channel and the dispersion channel, then **de-duplicates** their
events: within a cluster falling within roughly a minimum segment of each other it keeps only the most
significant, so a regime boundary that shifts both level and spread is reported once. The spike/dip stream is
added *after* de-duplication, because a point anomaly may legitimately coincide with a structural boundary
and should not be suppressed by it. Events are finally remapped from value-array indices back to
**source-bucket indices**, so empty buckets in the input are accounted for.

## Numerical stability and other notable choices

Real telemetry breaks naive implementations in a few specific ways, each handled deliberately:

- All structural cost/variance computation runs on values shifted by the global mean. Polynomial-with-intercept
  RSS is invariant to a constant shift in $y$, so this is mathematically a no-op, but it collapses working
  magnitudes from $O(level^2)$ to $O(spread^2)$ and removes the catastrophic cancellation in $E[y^2] − E[y]^2$
  that otherwise manufactures spurious breaks on near-constant high-magnitude series.
- Inside the verifier the regression x-coordinates are mapped affinely onto $[-1, 1]$. RSS is invariant under
  this reparametrisation, but the conditioning is not: raw indices push the moment matrix to $\approx x^(2·degree)$
  (e.g. $\approx 1e19$ for a cubic over a long window), which trips the singularity guard and silently degrades the
  fit to mean-only. On $[-1, 1]$ every moment is $O(1)$.
- A variance formed as $E[y^2] − E[y]^2$ carries rounding error of order $ulp(mean^2) ≈ |mean|\cdot ulp(|mean|)$,
  not $ulp(mean)^2$; the floors are linear in the ulp accordingly.
- The two scale choices are opposite on purpose. The down-weighting wants a *local* scale (suppress whatever
  is anomalous in its own neighbourhood); spike/dip detection wants a *global* scale (we genuinely care about
  global outliers). Using the wrong one in either place produces the characteristic failures — a missed spike
  on a low regime, or a high-variance regime reported as a run of spikes.
- Detection cost grows with the number of buckets, so a series longer than a cap (`MAX_SAMPLES`) is collapsed
  by `SeriesDownsampler` to two representatives per macro-bucket — the median and the largest-deviation extreme — 
  which preserves both the level and any spike. The downsampled series carries the original bucket indices, so
  every event still maps back to its true position; below the cap this is a no-op.
