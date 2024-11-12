import polars as pl


def run_langchain_analysis():
    delta_table_path = "delta-lake/coinbase-trade-data"

    # Use Polars to read from Delta Lake
    # Note: This assumes you've saved the table in Delta format
    df = pl.read_delta(delta_table_path)

    # Inspect the data
    print(df.head())

    from openai import OpenAI

    client = OpenAI(api_key="")

    # Convert df to a string for passing into the prompt
    df_str = str(df)  # Convert to string, omitting row indices
    # print(df_str)
    # OpenAI completion with context including df_str
    completion = client.chat.completions.create(
        model="gpt-4",
        messages=[
            {"role": "system", "content": f"You are a data scientist with the following dataset:\n{df_str}"},
            {
                "role": "user",
                "content": "Based on the context, what kind of questions should I ask you. It could be theory or analysis.",
            },
        ],
        temperature=0.7,
        max_tokens=500,
        top_p=1,
    )

    # Display the result
    print(completion)
    # print(completion['choices'][0]['message']['content'])
    # print(completion.choices[0].message['content'])



run_langchain_analysis()
