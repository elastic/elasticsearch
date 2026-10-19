
#ifndef VEC_COMMON_H
#define VEC_COMMON_H

#include <stdint.h>
#include <type_traits>
#include <utility>

template <uintptr_t align>
static inline uintptr_t align_downwards(const void* ptr) {
    static_assert(align > 0 && (align & (align - 1)) == 0, "Align must be a power of 2");

    uintptr_t addr = (uintptr_t)ptr;
    // Round down to align-byte boundary
    addr &= -align;
    return addr;
}

template <typename T, typename U>
static inline auto dot_scalar(const T a, const U b) {
    return a * b;
}

template <>
inline auto dot_scalar<uint16_t, uint16_t>(const uint16_t a, const uint16_t b) {
    const f32_t af = __builtin_bit_cast(f32_t, a << 16);
    const f32_t bf = __builtin_bit_cast(f32_t, b << 16);
    return af * bf;
}

template <>
inline auto dot_scalar<uint16_t, f32_t>(const uint16_t a, const f32_t b) {
    const f32_t af = __builtin_bit_cast(f32_t, a << 16);
    return af * b;
}

template <typename T, typename U>
static inline auto sqr_scalar(const T a, const U b) {
    auto d = a - b;
    return d * d;
}

template <>
inline auto sqr_scalar<uint16_t, uint16_t>(const uint16_t a, const uint16_t b) {
    const f32_t af = __builtin_bit_cast(f32_t, a << 16);
    const f32_t bf = __builtin_bit_cast(f32_t, b << 16);
    auto d = af - bf;
    return d * d;
}

template <>
inline auto sqr_scalar<uint16_t, f32_t>(const uint16_t a, const f32_t b) {
    const f32_t af = __builtin_bit_cast(f32_t, a << 16);
    auto d = af - b;
    return d * d;
}

// --- Mapper functions ---
// Each mapper resolves the i-th document vector to a direct pointer.
// All share the same signature so they can be passed as a template
// parameter to the bulk scoring templates (call_i8_bulk, call_f32_bulk, etc.).

// Contiguous layout: vectors are packed sequentially, each `pitch` elements apart.
template <typename T>
static inline const T* sequential_mapper(const T* data, const int32_t i, const int32_t* offsets, const int32_t pitch) {
    return data + (int64_t)i * pitch;
}

// Sparse layout: vectors reside in disjoint memory regions. `data` is an array
// of pre-resolved pointers — one per vector — so pitch and offsets are unused.
template <typename T>
static inline const T* sparse_mapper(const T* const* data, const int32_t i, const int32_t* offsets, const int32_t pitch) {
    return data[i];
}

// Position of the k-th (0-based) set bit of mask; k must be below popcount(mask).
// The k-th bit needs to be extracted from the middle of the mask, independently from other bits being set around it;
// our kernels do unrolling and prefetching, so the need to be able to access (and select) any vector at random
// inside the dataset.
// Defined per architecture (as different processors may or may not have native instructions for this operation).
static inline int32_t mask_select(const uint32_t mask, const int32_t k);

// Masked layout: a block of up to 32 vectors, each `pitch` elements apart, of which only those
// whose bit is set in the mask offsets[0] are scored; the i-th vector is the one at the position
// of the i-th set bit.
template <typename T>
static inline const T* masked_mapper(const T* data, const int32_t i, const int32_t* offsets, const int32_t pitch) {
    return data + (int64_t)mask_select((uint32_t)offsets[0], i) * pitch;
}

// --- Output mapper functions ---
// Each output mapper resolves the result slot of the i-th scored vector.
// All share the same signature so they can be passed as a template
// parameter to the bulk scoring templates, next to the (input) mapper.

// Compact output: the score of the i-th vector goes to results[i].
static inline int32_t compact_output(const int32_t* offsets, const int32_t i) {
    return i;
}

// Masked output: the score of the i-th vector goes to the position of the i-th set bit of
// offsets[0] (see masked_mapper).
static inline int32_t masked_output(const int32_t* offsets, const int32_t i) {
    return mask_select((uint32_t)offsets[0], i);
}

// Populates out[0..N-1] by invoking mapper for indices [base..base+N-1],
// with a bounds check that sets out-of-range entries to nullptr.
// Uses recursive template instantiation so each index is a compile-time constant.
template <int N, typename TData, typename T, const T*(*mapper)(const TData*, const int32_t, const int32_t*, const int32_t), int I = 0>
static inline void init_pointers(const T** out, const TData* a, int32_t pitch,
                                 const int32_t* offsets, int32_t base, int32_t count) {
    if constexpr (I < N) {
        out[I] = (base + I < count) ? mapper(a, base + I, offsets, pitch) : nullptr;
        init_pointers<N, TData, T, mapper, I + 1>(out, a, pitch, offsets, base, count);
    }
}

// Compile-time "loop" (analogous to a for-loop that is fully unrolled by the
// compiler). Generate calls to f(0), f(1), ..., f(N-1) where each index is a
// compile-time constant that can be used as a template argument or array subscript that
// the compiler resolves statically.
//
// The "loop" is implemented via compile-time recursion: the compiler recursively instantiates
// this function, effectively unrolling it. The net result is equivalent to writing f(0); f(1); ... f(N-1);
// inline, with no loops, branches, or function-call overhead in the compiled output.
template <int N, typename F, int I = 0>
static inline void apply_indexed(F&& f) {
    // This if is a compile-time branch: apply_indexed is compile-time recursive,
    // meaning the compiler will resolve the constexpr and instantiate the next iteration
    // of the template (apply_indexed<N, F, I + 1>).
    // Everything is handled by the compiler, no runtime branch is emitted.
    if constexpr (I < N) {
        // `std::integral_constant<int, I>` is a lightweight type used to wrap
        // a compile time constant (I). On the caller side, when the lambda parameter
        // is declared `auto I`, the compiler recursively instantiates this function and
        // deduces its type as integral_constant<int, 0>, then <int, 1>, etc.
        // This lets `I` be used wherever a compile-time constant is required.
        f(std::integral_constant<int, I>{});

        // `std::forward` is a C++ idiom related to modern C++ move semantics; it passes `f`
        // to the next recursive call preserving its original value category (lvalue
        // or rvalue). Here it simply avoids an unnecessary copy of the lambda.
        apply_indexed<N, F, I + 1>(std::forward<F>(f));
    }
}

// Performs a binary tree reduction on an array of values.
// Recurses, at compile time, to reduce N values to a single value
// using pairwise reduction whilst not introducing
// unneccessary data dependencies.
template <int N, typename T, T(*reduce)(const T, const T)>
static inline T tree_reduce(const T values[N]) {
    static_assert((N & (N - 1)) == 0, "N must be a power of 2");
    static_assert(N > 1, "There must be at least 2 values to reduce (N > 1)");

    if constexpr (N == 2) {
        return reduce(values[0], values[1]);
    } else {
        return reduce(
            tree_reduce<N/2, T, reduce>(values),
            tree_reduce<N/2, T, reduce>(values + N/2));
    }
}

#endif // VEC_COMMON_H
