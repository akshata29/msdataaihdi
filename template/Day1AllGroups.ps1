$Id = @()
$Upn = @()
$Name = @()
$Rg = @()

$DataIn = Import-Csv users1.csv -Delimiter ","

foreach ($d in $DataIn)
{
		$Id = $d.id
        $Upn = $d.userPrincipalName
		$Name = $d.displayName
		$Rg = $d.resourcegroup
		Write $Upn
		Write $Rg
		
		$args = @()
		$args += ("-uniqueName", $Rg)
		$args += ("-signInName", $Upn)
		$cmd = ".\Day1.ps1"

		#.\Day1.ps1 -uniqueName $Rg -signInName $Upn

		Invoke-Expression "$cmd $args"
		
		#break
}
#ForEach-Object {
#       
#    }