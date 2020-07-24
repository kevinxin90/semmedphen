from collections import defaultdict
import csv
import os
import sys

maxInt = sys.maxsize

DOC_TYPE = "PhenotypicFeature"
SEMMED_TYPE = "phenotypic_feature"

SEMMED_SEMANTIC_TYPE_MAPPING = {
    "chemical_substance": "ChemicalSubstance",
    "activity_and_behavior": None,
    "anatomical_entity": "AnatomicalEntity",
    "biological_entity": None,
    "biological_process_or_activity": "BiologicalProcess",
    "cell": "Cell",
    "cell_component": "CellularComponent",
    "disease_or_phenotypic_feature": "DiseaseOrPhenotypicFeature",
    "gene": "Gene",
    "genomic_entity": None,
    "gross_anatomical_structure": None,
    "phenotypic_feature": "PhenotypicFeature",
    "protein": "Gene"
}
SEMMED_PRED_MAPPING = {
    "ASSOCIATED_WITH": {
        "self": "related_to",
        "reverse": "related_to"
    },
    "INTERACTS_WITH": {
        "self": "physically_interacts_with",
        "reverse": "physically_interacts_with"
    },
    "AFFECTS": {
        "self": "affects",
        "reverse": "affected_by"
    },
    "STIMULATES": {
        "self": "positively_regulates",
        "reverse": "positively_regulated_by"
    },
    "INHIBITS": {
        "self": "negatively_regulates",
        "reverse": "negatively_regulated_by"
    },
    "DISRUPTS": {
        "self": "disrupts",
        "reverse": "disrupted_by"
    },
    "COEXISTS_WITH": {
        "self": "coexists_with",
        "reverse": "coexists_with"
    },
    "PREDISPOSES": {
        "self": "predisposes",
        "reverse": "predisposed_by"
    },
    "CAUSES": {
        "self": "causes",
        "reverse": "caused_by"
    },
    "TREATS": {
        "self": "treats",
        "reverse": "treated_by"
    },
    "PREVENTS": {
        "self": "prevents",
        "reverse": "prevented_by"
    },
    "OCCURS_IN": {
        "self": "occurs_in",
        "reverse": "in_which_occured"
    },
    "PROCESS_OF": {
        "self": "occurs_in",
        "reverse": "in_which_occured"
    },
    "LOCATION_OF": {
        "self": "location_of",
        "reverse": "located_in"
    },
    "PART_OF": {
        "self": "part_of",
        "reverse": "has_part"
    },
    "USES": {
        "self": "has_input",
        "reverse": "is_input_of"
    },
    "CONVERTS_TO": {
        "self": "derives_info",
        "reverse": "derives_from"
    },
    "MANIFESTATION_OF": {
        "self": "manifestation_of",
        "reverse": "manifested_by"
    },
    "PRODUCES": {
        "self": "produces",
        "reverse": "produced_by",
    },
    "PRECEDES": {
        "self": "precedes",
        "reverse": "preceded_by"
    },
    "ISA": {
        "self": "subclass_of",
        "reverse": "has_subclass"
    }
}

while True:
    # decrease the maxInt value by factor 10
    # as long as the OverflowError occurs.

    try:
        csv.field_size_limit(maxInt)
        break
    except OverflowError:
        maxInt = int(maxInt/10)


def load_data(data_folder):
    def construct_rec(sub_umls, obj_umls, line):
        if sub_umls not in gene_related:
            gene_related[sub_umls] = {
                '_id': sub_umls[5:],
                'umls': sub_umls[5:],
                'name': id_type_mapping[sub_umls]['name'],
                "@type": DOC_TYPE
            }
        pred = SEMMED_PRED_MAPPING[line[0]]['self']
        semantic_type = SEMMED_SEMANTIC_TYPE_MAPPING[id_type_mapping[obj_umls]['type']]
        if semantic_type:
            if pred not in gene_related[sub_umls]:
                gene_related[sub_umls][pred] = {}
            assoc = sub_umls + pred + str(line[1]) + obj_umls
            if assoc not in unique_assocs:
                unique_assocs.add(assoc)
                if obj_umls not in gene_related[sub_umls][pred]:
                    gene_related[sub_umls][pred][obj_umls] = {
                        "pmid": set(),
                        'umls': obj_umls[5:],
                        'name': id_type_mapping[obj_umls]['name'],
                        '@type': semantic_type
                    }
                gene_related[sub_umls][pred][obj_umls]["pmid"] = gene_related[sub_umls
                                                                              ][pred][obj_umls]["pmid"] | set(line[1].split(';'))
    nodes_path = os.path.join(data_folder, "nodes_neo4j.csv")
    edges_path = os.path.join(data_folder, "edges_neo4j.csv")
    group_by_semmantic_dict = defaultdict(list)
    id_type_mapping = {}
    unique_assocs = set()
    with open(nodes_path) as f:
        csv_reader = csv.reader(f, delimiter=',')
        next(csv_reader)
        for _item in csv_reader:
            group_by_semmantic_dict[_item[-2]].append(_item[-1])
            id_type_mapping[_item[-1]] = {'type': _item[-2], 'name': _item[1]}
    gene_related = {}
    with open(edges_path) as f:
        csv_reader = csv.reader(f, delimiter=',')
        next(csv_reader)
        for _item in csv_reader:
            if _item[4] in group_by_semmantic_dict[SEMMED_TYPE]:
                construct_rec(_item[4], _item[5], _item)
            elif _item[5] in group_by_semmantic_dict[SEMMED_TYPE]:
                construct_rec(_item[5], _item[4], _item)
    for v in gene_related.values():
        for m, n in v.items():
            if m not in ["_id", "umls", "name", "@type"]:
                v[m] = n.values()
        yield v
