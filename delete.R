settingsList <- list(
  ehr = settings_ehr,
  dod = settings_dod,
  ccae = settings_ccae
)

RiskStratifiedEstimation::prepareMultipleRseeViewer(
  analysisSettingsList = settingsList,
  saveDirectory = "."
)
