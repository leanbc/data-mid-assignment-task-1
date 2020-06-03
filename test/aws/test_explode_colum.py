import pandas as pd
from data_utils.aws.aws_helpers import explode_column

def test_explode_column():

    data='{"name":"joe", "surname":"doe", "job":"foe"}'
    df_test=pd.DataFrame([{'super_important_data': data}])
    df_test=explode_column(df_test,"super_important_data")


    df_expected=pd.DataFrame([{'super_important_data': data,'name':'joe','surname':'doe','job':'foe'}])

    assert df_test.values.all()==df_expected.values.all()




