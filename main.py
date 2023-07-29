import sys
import re
from bs4 import BeautifulSoup
import pandas as pd
from tabulate import tabulate
from hana_ml import dataframe
from itertools import tee
import datetime
import warnings

# from IPython.display import display, HTML




# Final_columns_keys = ['targetColumn', 'Mapping', 'schemaName', 'table',
#                      'tableField', 'coltype', 'isCalcColumn', 'formula', 'aggregationType', 'measureType',
#                      'comments']
# Final_columns_dict = {key: None for key in Final_columns_keys}
# Final_columns_df = pd.DataFrame()
# def pretty_print(df):
#     return display( HTML( df.to_html().replace("\\n","<br>") ) )

# def get_calc_formula_columns(p_calc_formula: str,sep: str):
#     regex = '\\'+sep+'(.*?)\\'+sep
#     calc_field_list = re.findall(regex, p_calc_formula)
#     return calc_field_list
def decode_direct_column(p_ViewXML, p_NodeXML, p_ViewNode, p_direct_targetColumn):
    # if p_NodeXML.find_all('mapping', {'target': p_direct_targetColumn}):
    mapping_list = p_NodeXML.find_all('input')
    # for map1 in (map1 for map1 in mapping_list if map1.get('node') == p_ViewNode):
    #     print(map1)
    for mapping in mapping_list:
        if mapping.get('node').replace('#', '') == p_ViewNode:
            # mapping = p_NodeXML.find('mapping', {'target': p_direct_targetColumn})

            if mapping is not None:
                input_node = p_ViewNode
                input_node = input_node.replace('#', '')
                if input_node.find('$') >= 0:
                    # input_node = input_node.replace(p_ViewNode, '', 1).replace('$', '')
                    input_node = p_ViewNode.replace(p_NodeXML.get('id'), '',1).replace('$', '')

                if mapping.find('mapping', {'target': p_direct_targetColumn}) is None:
                    if mapping.get('xsi:type') == 'Calculation:ConstantAttributeMapping':
                        print('Its union constant: ', mapping.get('value'))
                        pass
                        # return 'Its Union Constant'
                else:
                    input_column = mapping.find('mapping', {'target': p_direct_targetColumn}).get('source')
                    direct_col_mp = column_lineage(p_viewNodeId=input_node, p_columnName=input_column,
                                                   p_viewXML=p_ViewXML)
                    return direct_col_mp


def decode_calc_column(p_ViewXML, p_NodeXML, p_ViewNode, p_calc_targtColumn):
    final_calc_formula = None
    calc_column_xml = p_NodeXML.calculatedViewAttributes.find('calculatedViewAttribute', {'id': p_calc_targtColumn})
    if calc_column_xml is not None:
        regex = '\\"(.*?)\\"'
        calc_formula = calc_column_xml.get_text().replace('\n', '')
        # calc_formula = calc_formula.replace('"','')
        if final_calc_formula is None or final_calc_formula == '':
            final_calc_formula = calc_formula.replace('"','')
        # print('Its calculated column with formula: ', calc_formula)
        if re.findall(regex, calc_formula):
            calc_formula_fields = set(re.findall(regex, calc_formula)) # Use set to extract fields uniquely
            # for calc_column in re.findall(regex, calc_formula):  ## extract column names from Formula
            for calc_column in calc_formula_fields:
                if p_NodeXML.find('mapping', {'target': calc_column}):  ## Check if its direct column
                    # dd2 = decode_direct_column(p_ViewXML, p_NodeXML, p_ViewNode, calc_column)
                    input_node = p_NodeXML.get('id')

                    direct_col_mapping = column_lineage(input_node, calc_column, p_ViewXML)
                    if direct_col_mapping is not None:
                        if isinstance(direct_col_mapping, dict):
                            temp_formula = lambda q: direct_col_mapping.get(
                                'schema_name') + '.' + direct_col_mapping.get(
                                'table_name') + '->' + direct_col_mapping.get('field_name') if direct_col_mapping.get(
                                'schema_name') is not None and direct_col_mapping.get(
                                'table_name') is not None and direct_col_mapping.get(
                                'field_name') is not None else None
                            format_formula = temp_formula(direct_col_mapping)
                            final_calc_formula = final_calc_formula.replace(calc_column, str(format_formula))
                        # direct_col = direct_col_mapping.replace('"', '')
                        else:
                            final_calc_formula = final_calc_formula.replace(calc_column, str(direct_col_mapping))

                elif p_NodeXML.calculatedViewAttributes.find('calculatedViewAttribute', {'id': calc_column}):
                    node_id = p_NodeXML.get('id')
                    node_id = node_id.replace('#', '')
                    if node_id.find('$') >= 0:
                        node_id = node_id.replace(p_ViewNode, '', 1).replace('$', '')
                    calc_attr= column_lineage(node_id, calc_column, p_ViewXML)
                    # calc_attr = calc_attr.replace('"', '')
                    if calc_attr is not None:
                        if isinstance(calc_attr, dict):
                            temp_formula = lambda q: calc_attr.get('schema_name') + '.' + calc_attr.get(
                                'table_name') + '->' + calc_attr.get('field_name') if calc_attr.get(
                                'schema_name') is not None and calc_attr.get(
                                'table_name') is not None and calc_attr.get('field_name') is not None else None
                            format_formula = temp_formula(calc_attr)
                            final_calc_formula = final_calc_formula.replace(calc_column, str(format_formula))
                        else:
                            calc_attr = calc_attr.replace('"', '')
                            final_calc_formula = final_calc_formula.replace(calc_column, str(calc_attr))
                    # return final_calc_formula
            return final_calc_formula
        else:
            return final_calc_formula  ## formula does not contain base columns


def column_lineage(p_viewNodeId: str, p_columnName: str, p_viewXML):
    all_mapping_list = None
    if p_viewXML.dataSources.find('DataSource', {'id': p_viewNodeId, 'type': 'DATA_BASE_TABLE'}):
        base_mapping = {'schema_name': None, 'table_name': None, 'field_name': None}
        ds_table_xml = p_viewXML.dataSources.find('DataSource', {'id': p_viewNodeId, 'type': 'DATA_BASE_TABLE'})
        schema_name = ds_table_xml.columnObject.get('schemaName')
        field_name = ds_table_xml.columnObject.get('columnObjectName')
        base_mapping['schema_name'] = schema_name
        base_mapping['table_name'] = field_name
        base_mapping['field_name'] = p_columnName
        # tab_col = schema_name + '.' + table_name + '->' + p_columnName
        return base_mapping
        # return schema_name,table_name,p_columnName
    elif p_viewXML.dataSources.find('DataSource', {'id': p_viewNodeId, 'type': 'CALCULATION_VIEW'}):
        ds_view_xml = p_viewXML.dataSources.find('DataSource', {'id': p_viewNodeId, 'type': 'CALCULATION_VIEW'})
        package_name = ds_view_xml.resourceUri.get_text().split('/')[1]
        view_name = ds_view_xml.resourceUri.get_text().split('/')[3]
        data_source = view_lineage(p_packageName=package_name, p_viewName=view_name, p_viewColumn=p_columnName)
        # ds_return = package_name + '.' + view_name + '--' + p_columnName
        return data_source
    elif p_viewXML.dataSources.find('DataSource', {'id': p_viewNodeId, 'type': 'TABLE_FUNCTION'}):
        tf_text = 'its a table function: ' + p_viewNodeId
        # print(tf_text)
        field_name = 'TF'
        return p_viewNodeId
    elif p_viewXML.calculationViews.find('calculationView', {'id': p_viewNodeId}):
        node_xml = p_viewXML.calculationViews.find('calculationView', {'id': p_viewNodeId})
        if node_xml.find_all('mapping', {'target': p_columnName}):

            # if 'JoinView' in node_xml.get('xsi:type'):  ## Check if its Join Node
            #     pass
            #
            # # map_list = node_xml.find_all('mapping', {'target': p_columnName})
            # j_attr_list = []
            # for join_attr in (join_attr for join_attr in node_xml.find_all('joinAttribute') if
            #                   node_xml.find_all('joinAttribute') is not None):
            #     j_attr_list.append(join_attr.get('name'))
            map_list = (mapping for mapping in node_xml.find_all('mapping', {'target': p_columnName}) if
                        mapping.get('source') is not None)
            map_list1, map_list2 = tee(map_list)

            if 'JoinView' in node_xml.get('xsi:type'):  ## Check if its Join Node
                j_attr_list = []
                for join_attr in (join_attr for join_attr in node_xml.find_all('joinAttribute') if
                                  node_xml.find_all('joinAttribute') is not None):
                    j_attr_list.append(join_attr.get('name'))
                join_attr_flag = 'NO'
                for j_map in map_list2:
                   if j_map.get('source') in j_attr_list:
                       join_attr_flag = 'YES'
                       break


            for m in map_list1:
                input_node = m.parent.get('node')
                input_node = input_node.replace('#', '')
                input_column = m.get('target')
                # j_attr_list = []
                # for join_attr in (join_attr for join_attr in node_xml.find_all('joinAttribute') if
                #                   node_xml.find_all('joinAttribute') is not None):
                #     j_attr_list.append(join_attr.get('name'))
                if 'JoinView' in node_xml.get('xsi:type'):
                    if join_attr_flag == 'NO':
                        pass
                    elif input_column in j_attr_list:
                        if m.get('source') not in j_attr_list:
                            continue

                direct_col_source = decode_direct_column(p_ViewXML=p_viewXML, p_NodeXML=node_xml, p_ViewNode=input_node,
                                                         p_direct_targetColumn=input_column)

                if isinstance(direct_col_source, dict):
                    if direct_col_source.get('schema_name') is None or direct_col_source.get('table_name') is None:
                        continue
                    # Final_columns_dict['schemaName'] = direct_col_source.get('schema_name')
                    # if Final_columns_dict['table'] is None:
                    #     Final_columns_dict['table'] = direct_col_source.get('table_name')
                    #     Final_columns_dict['tableField'] = direct_col_source.get('field_name')
                    # else:
                    #     Final_columns_dict['table'] = Final_columns_dict['table']+ ' ; ' + str(direct_col_source.get('table_name'))
                    #     Final_columns_dict['tableField'] = Final_columns_dict['tableField']+ ' ; '+ str(direct_col_source.get('field_name'))

                    temp_formula = lambda q: direct_col_source.get('schema_name') + '.' + direct_col_source.get(
                        'table_name') + '->' + direct_col_source.get('field_name') if direct_col_source.get(
                        'schema_name') is not None and direct_col_source.get(
                        'table_name') is not None and direct_col_source.get(
                        'field_name') is not None else None
                    format_formula = temp_formula(all_mapping_list)

                    if all_mapping_list is None or all_mapping_list == '':
                        all_mapping_list = format_formula
                    else:
                        if node_xml.get('xsi:type') == 'Calculation:UnionView':
                            all_mapping_list = ' _Uinion_ '.join(
                                [x for x in (all_mapping_list, direct_col_source) if x])
                            # return all_mapping_list
                        else:
                            all_mapping_list = '|'.join([x for x in (all_mapping_list, format_formula) if x])

                    # Final_columns_dict['Mapping'] = all_mapping_list
                    # return all_mapping_list

                else:
                    if direct_col_source is None or direct_col_source == '':
                        continue
                    if all_mapping_list is None or all_mapping_list == '':
                        all_mapping_list = direct_col_source
                    else:
                        if node_xml.get('xsi:type') == 'Calculation:UnionView':
                            all_mapping_list = ' _Uinion_ '.join(
                                [x for x in (all_mapping_list, direct_col_source) if x])
                        else:

                            all_mapping_list = '|'.join([x for x in (all_mapping_list, direct_col_source) if x])
                    # Final_columns_dict['Mapping'] = all_mapping_list
                    # return all_mapping_list


            return all_mapping_list

        elif node_xml.calculatedViewAttributes.find('calculatedViewAttribute', {'id': p_columnName}):
            calc_attribute_source = decode_calc_column(p_ViewXML=p_viewXML, p_NodeXML=node_xml, p_ViewNode=p_viewNodeId,
                                                       p_calc_targtColumn=p_columnName)
            return calc_attribute_source


def view_lineage(p_packageName, p_viewName, p_viewColumn):
    df_cdata = df_all_view_xml.loc[
        (df_all_view_xml['PACKAGE_ID'] == p_packageName) & (df_all_view_xml['OBJECT_NAME'] == p_viewName), ['CDATA']]
    payload = list(x for x in df_cdata["CDATA"])[0]
    bs4_xml = BeautifulSoup(payload, features="xml")
    semantic_node_input = bs4_xml.logicalModel.get('id')
    semantic_node_xml = (bs4_xml.logicalModel.find(lambda tag: tag.get('id') == p_viewColumn))
    if semantic_node_xml is not None:
        if semantic_node_xml.parent.name == 'attributes':
            source_column = semantic_node_xml.keyMapping.get('columnName')
            col_mapping = column_lineage(p_viewNodeId=semantic_node_input, p_columnName=source_column,
                                         p_viewXML=bs4_xml)
            return col_mapping

        elif semantic_node_xml.parent.name == 'baseMeasures':
            source_column = semantic_node_xml.measureMapping.get('columnName')
            col_mapping = column_lineage(p_viewNodeId=semantic_node_input, p_columnName=source_column,
                                         p_viewXML=bs4_xml)
            return col_mapping

        elif semantic_node_xml.parent.name == 'calculatedAttributes':
            final_calc_formula = ''
            calculated_attribute_xml = bs4_xml.logicalModel.calculatedAttributes.find('calculatedAttribute',
                                                                                      {'id': p_viewColumn})
            if calculated_attribute_xml is not None:
                calc_attr_formula = calculated_attribute_xml.get_text().replace('\n', '')
                # calc_attr_formula = calc_attr_formula.replace('"','')
                if final_calc_formula is None or final_calc_formula == '':
                    final_calc_formula = calc_attr_formula.replace('"','')
                regex = '\\"(.*?)\\"'
                if re.findall(regex, calc_attr_formula):
                    for calc_attr_field in re.findall(regex, calc_attr_formula):
                        yz2 = column_lineage(p_viewNodeId=semantic_node_input, p_columnName=calc_attr_field,
                                             p_viewXML=bs4_xml)
                        final_calc_formula = final_calc_formula.replace(calc_attr_field, str(yz2))
                    return final_calc_formula
                else:
                    return calc_attr_formula

        elif semantic_node_xml.parent.name == 'calculatedMeasures':
            final_calc_formula = ''
            calculated_measure_xml = bs4_xml.logicalModel.calculatedMeasures.find('measure', {'id': p_viewColumn})
            if calculated_measure_xml is not None:
                calculated_measure_formula = calculated_measure_xml.get_text().replace('\n', '')
                # calculated_measure_formula = calculated_measure_formula.replace('"','')
                if final_calc_formula is None or final_calc_formula == '':
                    final_calc_formula = calculated_measure_formula.replace('"','')
                regex = '\\"(.*?)\\"'
                if re.findall(regex, calculated_measure_formula):
                    # calc_formula_fields = set(re.findall(regex, calculated_measure_formula))
                    for calc_measure_field in re.findall(regex, calculated_measure_formula):
                        yz2 = column_lineage(p_viewNodeId=semantic_node_input, p_columnName=calc_measure_field,
                                             p_viewXML=bs4_xml)
                        final_calc_formula = final_calc_formula.replace(calc_measure_field, str(yz2))
                    return final_calc_formula
                else:
                    return calculated_measure_formula

        elif semantic_node_xml.parent.name == 'restrictedMeasures':
            final_calc_formula = ''
            restricted_measure_xml = bs4_xml.logicalModel.restrictedMeasures.find('measure', {'id': p_viewColumn})
            if restricted_measure_xml is not None:
                restricted_measure_formula = restricted_measure_xml.get_text().replace('\n', '')
                # restricted_measure_formula = restricted_measure_formula.replace('"','')
                regex = '\\"(.*?)\\"'
                if re.findall(regex, restricted_measure_formula):
                    for res_formula_field in re.findall(regex, restricted_measure_formula):
                        yz2 = column_lineage(p_viewNodeId=semantic_node_input, p_columnName=res_formula_field,
                                             p_viewXML=bs4_xml)
                        final_calc_formula = final_calc_formula.replace(res_formula_field, str(yz2))
                    return final_calc_formula
                else:
                    return restricted_measure_formula


def parse_view_semantic(df_all_views_xml, p_parentView, p_parentPackage):
    view_semantic_keys = ['semanticNodeInput', 'targetColumn', 'sourceColumn', 'Mapping', 'schemaName', 'table',
                          'tableField', 'coltype', 'isCalcColumn', 'formula', 'aggregationType', 'measureType',
                          'comments']
    view_semantic = {key: None for key in view_semantic_keys}
    view_semantic_df = pd.DataFrame()
    df_parent_cdata = df_all_views_xml.loc[
        (df_all_views_xml['PACKAGE_ID'] == p_parentPackage) & (df_all_views_xml['OBJECT_NAME'] == p_parentView), [
            'CDATA']]
    payload = list(x for x in df_parent_cdata["CDATA"])[0]
    bs4_parent_xml = BeautifulSoup(payload, features="xml")
    parent_semantic_node_input = bs4_parent_xml.logicalModel.get('id')

    if bs4_parent_xml.logicalModel.find('attribute'):
        view_semantic = dict((k, None) for k in view_semantic)
        attributes_list = bs4_parent_xml.logicalModel.attributes.find_all('attribute')
        for attribute in attributes_list:
            view_semantic['semanticNodeInput'] = parent_semantic_node_input
            view_semantic['targetColumn'] = attribute.get('id')
            view_semantic['sourceColumn'] = attribute.keyMapping.get('columnName')
            view_semantic_df = view_semantic_df.append(view_semantic, ignore_index=True)

    if bs4_parent_xml.logicalModel.find('baseMeasures'):
        view_semantic = dict((k, None) for k in view_semantic)
        measures_list = bs4_parent_xml.logicalModel.baseMeasures.find_all('measure')
        for measure in measures_list:
            view_semantic['semanticNodeInput'] = parent_semantic_node_input
            view_semantic['targetColumn'] = measure.get('id')
            view_semantic['sourceColumn'] = measure.measureMapping.get('columnName')
            view_semantic_df = view_semantic_df.append(view_semantic, ignore_index=True)

    if bs4_parent_xml.logicalModel.find('calculatedAttributes'):
        ## Check if the column is a calculated attribute
        calculated_attributes_list = bs4_parent_xml.logicalModel.calculatedAttributes.find_all('calculatedAttribute')
        for calculatedAttribute in calculated_attributes_list:
            view_semantic = dict((k, None) for k in view_semantic)
            parent_calc_attr_formula = calculatedAttribute.get_text().replace('\n', '')
            view_semantic['semanticNodeInput'] = parent_semantic_node_input
            view_semantic['targetColumn'] = calculatedAttribute.get('id')
            view_semantic['isCalcColumn'] = 'Yes'
            view_semantic['formula'] = parent_calc_attr_formula
            view_semantic['coltype'] = 'Calculated Attribute Column'
            view_semantic_df = view_semantic_df.append(view_semantic, ignore_index=True)

            # regex = '\\"(.*?)\\"'
            # for parent_calc_attr_column in re.findall(regex, parent_calc_attr_formula):
            #     if view_semantic_df['targetColumn'].eq(parent_calc_attr_column).any():
            #         pass
            #     else:
            #         view_semantic['semanticNodeInput'] = parent_semantic_node_input
            #         view_semantic['sourceColumn'] = parent_calc_attr_column
            #         view_semantic['targetColumn'] = calculatedAttribute.get('id')
            #         view_semantic_df = view_semantic_df.append(view_semantic, ignore_index=True)

    if bs4_parent_xml.logicalModel.find('calculatedMeasures'):
        view_semantic = dict((k, None) for k in view_semantic)
        calculated_measures_list = bs4_parent_xml.logicalModel.calculatedMeasures.find_all('measure')
        for calculatedMeasure in calculated_measures_list:
            if calculatedMeasure.get('calculatedMeasureType') == 'counter':
                attributes = calculatedMeasure.find_all('attribute')
                parent_calc_measure_formula = 'Its a distinct Count of attributes:'
                for attribute in attributes:
                    parent_calc_measure_formula = parent_calc_measure_formula + ' ' + str(
                        attribute.get('attributeName'))
                view_semantic['semanticNodeInput'] = parent_semantic_node_input
                view_semantic['targetColumn'] = calculatedMeasure.get('id')
                view_semantic['isCalcColumn'] = 'Yes'
                view_semantic['formula'] = parent_calc_measure_formula
                view_semantic['coltype'] = 'Counter (Calculated measure)'
                # print('its a counter')
                view_semantic_df = view_semantic_df.append(view_semantic, ignore_index=True)

            if calculatedMeasure.get('semanticType') == 'amount':
                parent_calc_measure_formula = calculatedMeasure.formula.get_text().replace('\n', '')
                if calculatedMeasure.find('sourceCurrency'):
                    if calculatedMeasure.sourceCurrency.find('value'):
                        source_currency = calculatedMeasure.sourceCurrency.find('value').get_text()
                    elif calculatedMeasure.sourceCurrency.find('attribute'):
                        source_currency = calculatedMeasure.sourceCurrency.find('attribute').get('attributeName')
                if calculatedMeasure.find('targetCurrency'):
                    if calculatedMeasure.targetCurrency.find('value'):
                        target_currency = calculatedMeasure.targetCurrency.find('value').get_text()
                    elif calculatedMeasure.targetCurrency.find('attribute'):
                        target_currency = calculatedMeasure.targetCurrency.find('attribute').get('attributeName')
                if calculatedMeasure.find('referenceDate'):
                    if calculatedMeasure.referenceDate.find('value'):
                        reference_date = calculatedMeasure.referenceDate.find('value').get_text()
                    elif calculatedMeasure.referenceDate.find('attribute'):
                        reference_date = calculatedMeasure.referenceDate.find('attribute').get('attributeName')
                parent_calc_measure_formula = 'Its Calculated measure with currency conversion, Source currency = ', source_currency, \
                                              ' Target Currency =  ', target_currency, ' Reference date = ', reference_date, 'Source Column = ', parent_calc_measure_formula

                view_semantic['semanticNodeInput'] = parent_semantic_node_input
                view_semantic['targetColumn'] = calculatedMeasure.get('id')
                view_semantic['isCalcColumn'] = 'Yes'
                view_semantic['formula'] = parent_calc_measure_formula
                view_semantic['coltype'] = 'Calculated Measure '
                view_semantic_df = view_semantic_df.append(view_semantic, ignore_index=True)

    if bs4_parent_xml.logicalModel.find('restrictedMeasures'):
        rs_measures_list = bs4_parent_xml.logicalModel.restrictedMeasures.find_all('measure')
        for rs_measure in rs_measures_list:
            base_measure = rs_measure.get('baseMeasure').replace('#', '')
            parent_rs_measure_formula = 'Its a restricted Measure with base measure :' + base_measure + ' and restricted by attributes: '
            restrictions = rs_measure.find_all('restriction')
            for restriction in restrictions:
                parent_rs_measure_formula = parent_rs_measure_formula + ' ' + restriction.filter.get('attributeName')

            view_semantic['semanticNodeInput'] = parent_semantic_node_input
            view_semantic['targetColumn'] = rs_measure.get('id')
            view_semantic['isCalcColumn'] = 'Yes'
            view_semantic['formula'] = parent_rs_measure_formula
            view_semantic['coltype'] = 'Restricted Measure Column'
            view_semantic_df = view_semantic_df.append(view_semantic, ignore_index=True)

    # print(tabulate(view_semantic_df, headers='keys', tablefmt='psql'))
    if view_semantic_df is not None:
        view_semantic_df.reset_index()
        for row in view_semantic_df.itertuples(index=True, name='Pandas'):

            # global Final_columns_df
            # global Final_columns_dict
            # Final_columns_dict['targetColumn'] = getattr(row, 'targetColumn')
            # Final_columns_df = Final_columns_df.append(Final_columns_dict, ignore_index=True)
            # # Final_columns_dict = dict((k, None) for k in Final_columns_dict)
            # Final_columns_dict['targetColumn'] = None
            # Final_columns_dict['Mapping'] = None
            # Final_columns_dict['schemaName'] = None
            # Final_columns_dict['table'] = None
            # Final_columns_dict['tableField'] = None


            if getattr(row, 'isCalcColumn')== 'Yes':
                continue
            else:
                semantic_node_input = getattr(row, 'semanticNodeInput')
                source_column = getattr(row, 'sourceColumn')
                # node_xmlelement = bs4_xml.calculationViews.find('calculationView',{'id':semantic_node})
                if source_column is not None:
                    pass



                semantic_col_mapping = column_lineage(p_viewNodeId=semantic_node_input, p_columnName=source_column,
                                                      p_viewXML=bs4_parent_xml)

                # print('\n',Final_columns)

                if semantic_col_mapping is not None:
                    if isinstance(semantic_col_mapping, dict):
                        view_semantic_df.at[row.Index, "schemaName"] = semantic_col_mapping.get('schema_name')
                        view_semantic_df.at[row.Index, "table"] = semantic_col_mapping.get('table_name')
                        view_semantic_df.at[row.Index, "tableField"] = semantic_col_mapping.get('field_name')
                        view_semantic_df.at[row.Index, "Mapping"] = semantic_col_mapping
                        # semantic_col_mapping.clear()
                    else:
                        view_semantic_df.at[row.Index, "Mapping"] = semantic_col_mapping.strip()
                        # semantic_col_mapping = None
                    # print(semantic_col_mapping)

        # pd.options.display.width = None
        # print(view_semantic_df.to_string())
        # print(pretty_print(view_semantic_df))
        view_semantic_df_copy = view_semantic_df[['targetColumn','Mapping','formula','isCalcColumn','schemaName','table','tableField']]
        # for num,row in view_semantic_df_copy.iterrows():
        #     print(row)
        # print(view_semantic_df_copy.head(n=250).to_string(index=False))

        # print(tabulate(view_semantic_df_copy, headers='keys', tablefmt='psql'))
        print('*****')
        return view_semantic_df_copy
        # print(tabulate(Final_columns_df, headers='keys', tablefmt='psql'))

        # view_semantic_df_copy.to_csv('final_output', header=True, sep=':')
def display_menu():
    print("COMMAND MENU")
    print("1   - Lineage for 1 view")
    print("2 - Entire Package")
    print("3   - Exit program")


if __name__ == '__main__':
    warnings.filterwarnings("ignore", category=FutureWarning)
    # HANA_SQL_SCRIPT = ''' upsert "NGUMATIMA1"."LINEAGE" ("PACKAGENAME",
    # "VIEWNAME",
    # "TARGETCOLUMN",
    # "MAPPING",
    # "META_CRT_DT") values(?,?,?,?,?)  WITH PRIMARY KEY
    # '''
    HANA_SQL_SCRIPT = ''' upsert "CSTM_ILMN_P2D"."LINEAGE" ("MAPPING","META_CRT_DT","PACKAGENAME","TARGETCOLUMN","VIEWNAME") values(?,?,?,?,?)   WITH PRIMARY KEY  '''

    view_query = '''
    select distinct
    package_id,
    object_name 

    from "_SYS_REPO"."ACTIVE_OBJECT" 
    where (package_id like 'ILMN.%P2D%' or package_id like 'ILMN.%P2P%')
    and object_name like '%_QV'
    '''
    HANA_wa_list = ['PACKAGENAME','VIEWNAME','TARGETCOLUMN','MAPPING','META_CRT_DT']
    HANA_wa_dict = {key: None for key in HANA_wa_list}
    # df_hana = pd.DataFrame()
    cc = dataframe.ConnectionContext('analyticsdev.illumina.com', 30041, 'ngumatima1','DevABC#9797')
    # cc = dataframe.ConnectionContext('analyticsdev.illumina.com', 30041,"","")

    def start_lineage(p_path):
        global df_all_view_xml
        df_hana = pd.DataFrame()
        with open(r'H_SQL', 'r') as file:
            view_query = file.read()
            view_query = view_query.replace('!viewPath!', viewPath)
        with open(r'Column_query', 'r') as file:
            column_query = file.read()
            column_query = column_query.replace('!viewPath!', viewPath)

        # cc = dataframe.ConnectionContext('analyticsqas.illumina.com', 30041, 'ngumatima1')
        if view_query is not None:
            hdf_view = cc.sql(view_query)
            df_all_view_xml = hdf_view.collect()
            # pd.options.display.width = None
            # print(df_all_view_xml)
            # with pd.option_context('expand_frame_repr', False, 'display.max_rows', None):
            #     print(df_all_view_xml)

        else:
            print('sql query has issue')
            sys.exit(0)

        if column_query is not None:
            hdf_col = cc.sql(column_query)
            df_col = hdf_col.collect()
        else:
            print('sql query has issue')
            sys.exit(0)
        # cc.connection.close()

        df_final = parse_view_semantic(df_all_views_xml=df_all_view_xml, p_parentPackage=packageName,
                                       p_parentView=viewName)
        # print(tabulate(df_final, headers='keys', tablefmt='psql'))
        print('*********')
        for row in df_final.itertuples(index=True, name='Pandas'):
            HANA_wa_dict['PACKAGENAME'] = packageName
            HANA_wa_dict['VIEWNAME'] = viewName
            HANA_wa_dict['TARGETCOLUMN'] = getattr(row, 'targetColumn')
            if getattr(row, 'isCalcColumn') is None or getattr(row, 'isCalcColumn') == '':
                HANA_wa_dict['MAPPING'] = getattr(row, 'Mapping')
            else:
                HANA_wa_dict['MAPPING'] = getattr(row, 'formula')
            HANA_wa_dict['META_CRT_DT'] = datetime.datetime.now()
            df_hana = df_hana.append(HANA_wa_dict, ignore_index=True)
            # df_hana = pd.concat([df_hana,HANA_wa_dict], ignore_index=True)

        print(tabulate(df_hana, headers='keys', tablefmt='psql'))

        ###--- Insert data into HANA table
        hana_cur = cc.connection.cursor()
        chunk = df_hana.iloc[0: 1000].values.tolist()
        tuple_of_tuples = list(tuple(x) for x in chunk)
        try:
          hana_cur.executemany(HANA_SQL_SCRIPT, tuple_of_tuples)
          print('HANA Table updated with Lineage')
        except Exception as e:
            print(viewPath,'\n')
            print(e)


    display_menu()
    while True:
        command = input("\nCommand: ")
        if command == '1':
            packageName = str(input('Enter HANA Package Name: '))
            packageName = packageName.replace(' ', '')
            viewName = str(input('Enter HANA View Name: '))
            viewName = viewName.replace(' ', '')
            viewPath = packageName + '/' + viewName
            start_lineage(p_path=viewPath)
            print('\n Lineage is completed')
            sys.exit(0)

        elif command == '3':
            sys.exit(0)
        elif command == '2':
            hdf = cc.sql(view_query)
            df_views = hdf.collect()
            for row in df_views.itertuples(index=True, name='Pandas'):
                packageName = getattr(row, 'PACKAGE_ID')
                viewName = getattr(row, 'OBJECT_NAME')
                viewPath = packageName + '/' + viewName
                start_lineage(p_path=viewPath)
            print('\n Lineage is completed')
            sys.exit(0)


    # packageName = str(input('Enter HANA Package Name: '))
    # packageName = packageName.replace(' ', '')
    # viewName = str(input('Enter HANA View Name: '))
    # viewName = viewName.replace(' ', '')
    # viewPath = packageName + '/' + viewName

    #     with open(r'H_SQL', 'r') as file:
    #         view_query = file.read()
    #         view_query = view_query.replace('!viewPath!', viewPath)
    #     with open(r'Column_query', 'r') as file:
    #         column_query = file.read()
    #         column_query = column_query.replace('!viewPath!', viewPath)
    #
    #     # cc = dataframe.ConnectionContext('analyticsqas.illumina.com', 30041, 'ngumatima1', 'NGqas#2597')
    #     if view_query is not None:
    #         hdf_view = cc.sql(view_query)
    #         df_all_view_xml = hdf_view.collect()
    #         # pd.options.display.width = None
    #         # print(df_all_view_xml)
    #         # with pd.option_context('expand_frame_repr', False, 'display.max_rows', None):
    #         #     print(df_all_view_xml)
    #
    #     else:
    #         print('sql query has issue')
    #         sys.exit(0)
    #
    #     if column_query is not None:
    #         hdf_col = cc.sql(column_query)
    #         df_col = hdf_col.collect()
    #     else:
    #         print('sql query has issue')
    #         sys.exit(0)
    #     # cc.connection.close()
    #
    #     df_final = parse_view_semantic(df_all_views_xml=df_all_view_xml, p_parentPackage=packageName, p_parentView=viewName)
    #     print(tabulate(df_final, headers='keys', tablefmt='psql'))
    #     print('*********')
    #     for row in df_final.itertuples(index=True, name='Pandas'):
    #         HANA_wa_dict['PACKAGENAME']= packageName
    #         HANA_wa_dict['VIEWNAME']=viewName
    #         HANA_wa_dict['TARGETCOLUMN']=getattr(row, 'targetColumn')
    #         if getattr(row, 'isCalcColumn') is None or getattr(row, 'isCalcColumn')=='':
    #            HANA_wa_dict['MAPPING']=getattr(row, 'Mapping')
    #         else:
    #            HANA_wa_dict['MAPPING'] = getattr(row, 'formula')
    #         HANA_wa_dict['META_CRT_DT']=datetime.datetime.now()
    #         df_hana = df_hana.append(HANA_wa_dict, ignore_index=True)
    #         # df_hana = pd.concat([df_hana,HANA_wa_dict], ignore_index=True)
    #
    #     print(tabulate(df_hana, headers='keys', tablefmt='psql'))
    #
    # ###--- Insert data into HANA table
    #     hana_cur = cc.connection.cursor()
    #     chunk = df_hana.iloc[0: 1000].values.tolist()
    #     tuple_of_tuples = list(tuple(x) for x in chunk)
    #     hana_cur.executemany(HANA_SQL_SCRIPT, tuple_of_tuples)


    print('\n', 'done')

