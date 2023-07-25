import sys
import papermill as pm
import os


name = sys.argv[1]+'-report.ipynb'

res = pm.execute_notebook(
    'weekly-report.ipynb',
    name,
    parameters=dict(week2=sys.argv[1])
)

command = 'jupyter nbconvert --no-input --to html ' + name

os.system(command)
