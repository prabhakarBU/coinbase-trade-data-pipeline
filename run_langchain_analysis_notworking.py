# from langchain.chains import LLMChain
# from langchain.llms import OpenAI
# from langchain.prompts import PromptTemplate

from langchain_community.llms import OpenAI
from langchain_community.chains import LLMChain
from langchain_community.prompts import PromptTemplate

import polars as pl

# Define your OpenAI API key
import openai
openai.api_key = ""

def run_langchain_analysis():
    delta_table_path = "delta-lake/coinbase-trade-data"
    
    # Use Polars to read from Delta Lake
    # Note: This assumes you've saved the table in Delta format
    df = pl.read_delta(delta_table_path)

    # Inspect the data
    print(df.head())

    # Setup OpenAI model using LangChain
    llm = OpenAI(model="gpt-4-turbo")

    # Optionally, you can create custom prompts
    prompt_template = "Analyze the following query and provide insights: {query}"
    prompt = PromptTemplate(input_variables=["query"], template=prompt_template)

    # Query to analyze (this could come from user input)
    query = "How many rows have a value greater than 100 in the 'price' column?"

    # Setup the LLMChain
    chain = LLMChain(llm=llm, prompt=prompt)

    # Use LangChain to analyze the query
    response = chain.run(query=query)

    print("Query analysis:", response)