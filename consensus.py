import pandas as pd
import numpy as np
from os.path import join


def prepare_compiled(fn='compiled.csv'):
    df = pd.read_csv(fn)
    df = df[df.columns[:-1]]
    df.set_index('Source', inplace=True)
    df.columns = [c[10:] for c in df.columns]
    # remove column Melissa Zimdars
    df.drop('Melissa Zimdars', axis=1, inplace=True)
    # remove orphans
    n = df.replace(['Yes', 'No'], [1, 0]).sum(axis=1)
    df = df.loc[n>0]
    return df.reset_index(drop=False)


def prepare_kate_starbird(fn='kate_starbird.csv'):
    df = pd.read_csv(fn)
    return df[df.columns[0]].str.lower().drop_duplicates().tolist()


def prepare_open_sources(fn='opensources.csv'):
    claim_tags = [
        'fake', 'satire', 'bias', 'conspiracy', 'rumor', 'state', 'junksci',
        'hate', 'clickbait'
    ]
    df = pd.read_csv(
        fn,
        header=0,
        usecols=range(5),
        names=['domain', 'type1st', 'type2nd', 'type3rd', 'notes'])
    df = df.loc[(df.type1st.isin(claim_tags)) | (df.type2nd.isin(claim_tags)) |
                (df.type3rd.isin(claim_tags))]
    return df['domain'].str.lower().drop_duplicates().tolist()


def prepare_craig_silverman(fndir='craig_silverman/data'):
    df1 = pd.read_csv(join(fndir, 'sites_2016.csv'))
    df2 = pd.read_csv(join(fndir, 'sites_2017.csv'))
    return df1['domain'].append(
        df2['domain'],
        ignore_index=True).str.lower().drop_duplicates().tolist()


def prepare_politifact(fn='politifact.csv'):
    df = pd.read_csv(fn)
    return df['Site name'].str.lower().drop_duplicates().tolist()


def consensus(to_fn='consensus.csv'):
    import pdb
    pdb.set_trace()  # XXX BREAKPOINT
    compiled_df = prepare_compiled()
    compiled = compiled_df['Source'].tolist()
    kate_starbird = prepare_kate_starbird()
    open_sources = prepare_open_sources()
    craig_silverman = prepare_craig_silverman()
    politifact = prepare_politifact()

    unioned = set(compiled) | set(kate_starbird) |\
        set(open_sources) | set(craig_silverman) |\
        set(politifact)
    consensus = pd.DataFrame(list(unioned), columns=['Source'])
    consensus = pd.merge(consensus, compiled_df, how='left', on='Source')
    consensus.set_index('Source', inplace=True)
    consensus.loc[kate_starbird, 'Kate Starbird'] = 'Yes'
    consensus.loc[open_sources, 'Opensources'] = 'Yes'
    consensus.loc[craig_silverman, 'Craig Silverman'] = 'Yes'
    consensus.loc[politifact, 'Politifact'] = 'Yes'
    consensus = consensus.fillna('No')

    consensus_b = consensus.replace(to_replace=['Yes', 'No'], value=[1, 0])
    consensus['n_consensus'] = consensus_b.sum(axis=1)

    consensus.to_csv(to_fn)
