#%%
import polars as pl

df = pl.read_csv("data/tsla_tweets.csv")

#%%
df.head(2)

#%%
def add_n_cashtags(data: pl.DataFrame) -> pl.DataFrame:
    return data.with_column(
        pl.col("cashtags")
        .str.replace(r"\[", "")
        .str.replace(r"\]", "")
        .str.split(", ")
        .arr.lengths()
        .alias("n_cashtags")
    )


def remove_links(data: pl.DataFrame) -> pl.DataFrame:
    return data.with_column(
        pl.col("tweet").str.replace_all(r"@\S+|https?://\S+|pic\.twitter\.com/\S+", "")
    )


def n_cashtags_filter(data: pl.DataFrame, n: int = 4) -> pl.DataFrame:
    return data.filter(pl.col("n_cashtags") <= n)


def remove_newlines(data: pl.DataFrame) -> pl.DataFrame:
    return data.with_column(pl.col("tweet").str.replace_all(r"\n", " "))


clean = df.pipe(add_n_cashtags).pipe(remove_links).pipe(n_cashtags_filter)

#%%
with open("data/clean_tweets.txt", "w") as f:
    f.writelines("\n".join(df.tweet))
